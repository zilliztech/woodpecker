package cmd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

type shellTenant struct {
	name, bucket, rootPath string
	etcd                   config.EtcdConfig
}

type shellSession struct {
	file        *config.File
	configPath  string
	contextName string
	service     config.Context
	tenant      *shellTenant
	preferences []string
	in          io.Reader
	out, errOut io.Writer
	// Tests can exercise session transitions and dispatch without starting another CLI.
	checkTenant func(context.Context, *shellTenant) error
	runChild    func(context.Context, []string, string) error
}

func (s *shellSession) tenantName() string {
	if s.tenant == nil {
		return "disconnected"
	}
	return s.tenant.name
}

func validShellName(name string) bool {
	return name != "" && !strings.ContainsAny(name, "[]|>") && strings.IndexFunc(name, unicode.IsControl) < 0
}

func (s *shellSession) use(name string) error {
	if !validShellName(name) {
		return wperrors.NewUsageError("context names must be nonempty and contain no control characters or []|>")
	}
	c, err := s.file.ResolveContext(name)
	if err != nil {
		return shellConfigError(err, s.configPath)
	}
	// Copy the map: startup overrides and later commands cannot change the loaded
	// configuration, another context, or another shell session.
	mappings := make(map[string]string, len(c.NodeAdminURLs))
	for key, value := range c.NodeAdminURLs {
		mappings[key] = value
	}
	c.NodeAdminURLs = mappings
	c.Etcd = config.EtcdConfig{}
	previous := s.service
	s.service = c
	if err := s.validateService(); err != nil {
		s.service = previous
		return err
	}
	s.contextName, s.tenant = name, nil
	return nil
}

func (s *shellSession) validateService() error {
	origin, err := client.ValidateNodeAdminURLs(map[string]string{"service": s.service.Endpoint})
	if err != nil {
		return shellConfigError(fmt.Errorf("service endpoint must be an HTTP(S) origin without credentials, path, query, or fragment"), s.configPath)
	}
	s.service.Endpoint = origin["service"]
	mappings, err := client.ValidateNodeAdminURLs(s.service.NodeAdminURLs)
	if err != nil {
		return shellConfigError(err, s.configPath)
	}
	s.service.NodeAdminURLs = mappings
	return nil
}

func (s *shellSession) timeout() time.Duration {
	for _, arg := range s.preferences {
		if value, ok := strings.CutPrefix(arg, "--timeout="); ok {
			if timeout, err := time.ParseDuration(value); err == nil && timeout > 0 {
				return timeout
			}
		}
	}
	if s.service.Timeout > 0 {
		return s.service.Timeout
	}
	return 30 * time.Second
}

func (s *shellSession) addCommands(root *cobra.Command) {
	for _, c := range root.Commands() {
		if c.Name() == "shell" {
			root.RemoveCommand(c)
		}
	}
	root.AddCommand(&cobra.Command{Use: "use <context>", Short: "Switch service context for this session and detach the tenant", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if err := s.use(args[0]); err != nil {
			return err
		}
		s.printStatus(false, cmd.Context())
		return nil
	}})
	root.AddCommand(&cobra.Command{Use: "disconnect", Short: "Clear the tenant connection and retain the service context", Args: cobra.NoArgs, Run: func(_ *cobra.Command, _ []string) {
		s.tenant = nil
		fmt.Fprintln(s.out, "Tenant metadata: not configured")
	}})
	status := &cobra.Command{Use: "status", Short: "Show effective session configuration; --check also checks Admin and etcd", Args: cobra.NoArgs}
	var check bool
	status.Flags().BoolVar(&check, "check", false, "Run bounded Admin and etcd reachability checks")
	status.RunE = func(cmd *cobra.Command, _ []string) error { return s.printStatus(check, cmd.Context()) }
	root.AddCommand(status, s.connectCommand())
	root.AddCommand(&cobra.Command{Use: "exit", Aliases: []string{"quit"}, Short: "Leave this shell", Args: cobra.NoArgs, Run: func(*cobra.Command, []string) {}})
}

func (s *shellSession) connectCommand() *cobra.Command {
	var flags metaEtcdFlags
	var bucket, rootPath string
	cmd := &cobra.Command{Use: "connect <tenant>", Short: "Validate and replace the tenant metadata connection", Args: cobra.ExactArgs(1)}
	flags.register(cmd)
	cmd.Flags().StringVar(&bucket, "bucket", "", "Storage bucket (pair with --root-path)")
	cmd.Flags().StringVar(&rootPath, "root-path", "", "Storage instance root path (pair with --bucket)")
	_ = cmd.MarkFlagRequired("etcd")
	_ = cmd.MarkFlagRequired("meta-prefix")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if !validShellName(args[0]) || args[0] == "disconnected" {
			return wperrors.NewUsageError("tenant names must be nonempty, not 'disconnected', and contain no control characters or []|>")
		}
		if strings.TrimSpace(flags.metaPrefix) == "" || strings.Trim(flags.metaPrefix, "/") == "" {
			return wperrors.NewUsageError("--meta-prefix must specify the complete, nonempty tenant metadata prefix")
		}
		if (bucket == "") != (rootPath == "") {
			return wperrors.NewUsageError("--bucket and --root-path must be provided together")
		}
		candidate := &shellTenant{name: args[0], bucket: bucket, rootPath: rootPath, etcd: config.EtcdConfig{
			MetaPrefix: flags.metaPrefix, Username: flags.etcdUsername, Password: flags.etcdPassword,
			Cert: flags.etcdCert, Key: flags.etcdKey, CACert: flags.etcdCACert, TLSMinVersion: flags.etcdTLSMinVersion,
		}}
		for _, endpoint := range strings.Split(flags.etcdEndpoints, ",") {
			endpoint = strings.TrimSpace(endpoint)
			if endpoint == "" {
				return wperrors.NewUsageError("--etcd must contain nonempty endpoints")
			}
			origin := endpoint
			if !strings.Contains(origin, "://") {
				origin = "http://" + origin
			}
			if _, err := client.ValidateNodeAdminURLs(map[string]string{"etcd": origin}); err != nil {
				return wperrors.NewUsageError("invalid etcd endpoint; pass credentials using --etcd-username and --etcd-password")
			}
			candidate.etcd.Endpoints = append(candidate.etcd.Endpoints, endpoint)
		}
		if (candidate.etcd.Cert == "") != (candidate.etcd.Key == "") {
			return wperrors.NewUsageError("--etcd-cert and --etcd-key must be provided together")
		}
		if candidate.etcd.Password != "" && candidate.etcd.Username == "" {
			return wperrors.NewUsageError("--etcd-password requires --etcd-username")
		}
		check := s.checkTenant
		if check == nil {
			check = s.checkEtcd
		}
		if err := check(cmd.Context(), candidate); err != nil {
			return fmt.Errorf("tenant connection failed (etcd reachability, authentication, or TLS): %w; active tenant unchanged: %s", err, s.tenantName())
		}
		s.tenant = candidate
		return s.printStatus(false, cmd.Context())
	}
	return cmd
}

func (s *shellSession) checkEtcd(parent context.Context, tenant *shellTenant) error {
	e := tenant.etcd
	conn := &metaEtcdConn{
		endpoints: e.Endpoints, username: e.Username, password: e.Password,
		tlsCert: e.Cert, tlsKey: e.Key, tlsCACert: e.CACert, tlsMinVer: e.TLSMinVersion,
		useSSL: e.Cert != "" || e.Key != "" || e.CACert != "",
	}
	for _, endpoint := range e.Endpoints {
		if strings.HasPrefix(endpoint, "https://") {
			conn.useSSL = true
		}
	}
	ctx, cancel := context.WithTimeout(parent, s.timeout())
	defer cancel()
	cli, err := metaEtcdClientForContext(ctx, conn, s.timeout())
	if err != nil {
		return err
	}
	defer cli.Close()
	// Read a bounded slice of the actual namespace: unlike maintenance Status,
	// this also validates read authorization for a tenant-scoped etcd user.
	_, err = cli.Get(ctx, strings.TrimRight(e.MetaPrefix, "/")+"/", clientv3.WithPrefix(), clientv3.WithLimit(1))
	return err
}

func safeShellEndpoint(endpoint string) string {
	schemeless := !strings.Contains(endpoint, "://")
	if schemeless {
		endpoint = "http://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "[invalid endpoint]"
	}
	if u.User != nil {
		u.User = url.User("REDACTED")
	}
	if u.RawQuery != "" {
		u.RawQuery = "REDACTED"
	}
	u.Fragment = ""
	if schemeless {
		return strings.TrimPrefix(u.String(), "http://")
	}
	return u.String()
}

func (s *shellSession) printStatus(check bool, parent context.Context) error {
	fmt.Fprintf(s.out, "Service context: %s\nConfig file:     %s\nAdmin endpoint:  %s\nNode mappings:   %d\n", s.contextName, s.configPath, safeShellEndpoint(s.service.Endpoint), len(s.service.NodeAdminURLs))
	if s.tenant == nil {
		fmt.Fprintln(s.out, "Tenant metadata: not configured")
	} else {
		t := s.tenant
		endpoints := make([]string, len(t.etcd.Endpoints))
		for i, e := range t.etcd.Endpoints {
			endpoints[i] = safeShellEndpoint(e)
		}
		fmt.Fprintf(s.out, "Tenant:          %s\nEtcd endpoints:  %s\nMeta prefix:     %s\nBucket:          %s\nRoot path:       %s\n", t.name, strings.Join(endpoints, ", "), t.etcd.MetaPrefix, t.bucket, t.rootPath)
		if t.etcd.Username != "" || t.etcd.Password != "" {
			fmt.Fprintln(s.out, "Etcd auth:       configured (credentials redacted)")
		}
	}
	if !check {
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, s.timeout())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(s.service.Endpoint, "/")+"/admin/memberlist", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	response, adminErr := http.DefaultClient.Do(req)
	if adminErr == nil {
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			adminErr = fmt.Errorf("HTTP %d", response.StatusCode)
		}
	}
	printShellCheck(s.out, "Admin", adminErr)
	var etcdErr error
	if s.tenant == nil {
		fmt.Fprintln(s.out, "Etcd check: skipped (no tenant configured)")
	} else {
		etcdErr = s.checkEtcd(parent, s.tenant)
		printShellCheck(s.out, "Etcd", etcdErr)
	}
	if adminErr != nil || etcdErr != nil {
		return wperrors.NewNetworkError("reachability check failed; selected session configuration retained")
	}
	return nil
}

func printShellCheck(out io.Writer, name string, err error) {
	if err != nil {
		fmt.Fprintf(out, "%s check: failed (%v)\n", name, err)
	} else {
		fmt.Fprintf(out, "%s check: OK\n", name)
	}
}
