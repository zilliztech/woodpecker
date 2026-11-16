package webui

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	diagcommand "github.com/zilliztech/woodpecker/diagnostics/command"
)

// loadWorkspaceSource captures startup configuration once. Browsers subsequently
// own their selections; neither file changes nor ambient environment retarget jobs.
func loadWorkspaceSource(path, name, sessionPath string) (*config.File, string, *config.SessionConfig, error) {
	if sessionPath == "" {
		f, abs, err := diagcommand.LoadDocument(path)
		return f, abs, nil, err
	}
	session, err := config.LoadSession(sessionPath)
	if err != nil {
		return nil, "", nil, err
	}
	if session.ClientStorage != nil {
		if err := session.ClientStorage.Validate(); err != nil {
			return nil, "", nil, err
		}
	}
	if session.ClientMeta != nil {
		e := session.ClientMeta
		if _, err := normalizeMetadata(metadataSession{Etcd: strings.Join(e.Endpoints, ","), Prefix: e.MetaPrefix}); err != nil {
			return nil, "", nil, fmt.Errorf("invalid session client_meta: %w", err)
		}
		if strings.Trim(e.MetaPrefix, "/ .") == "" || e.Password != "" && e.Username == "" || (e.Cert == "") != (e.Key == "") {
			return nil, "", nil, fmt.Errorf("invalid session client_meta prefix or authentication settings")
		}
	}
	if path != "" || name != "" {
		f, abs, err := diagcommand.LoadDocument(path)
		return f, abs, session, err
	}
	// Metadata-only and observability-only workspaces do not require WP Admin.
	service := config.Context{}
	if session.ServiceContext != nil {
		service = *session.ServiceContext
	}
	view := config.SessionConfig{ServiceContext: &service}
	f, err := view.ServiceFile()
	if err != nil {
		return nil, "", nil, err
	}
	abs, err := filepath.Abs(sessionPath)
	return f, abs, session, err
}

func (v *browserSession) attachConfiguration(initial *config.SessionConfig) {
	if initial == nil {
		return
	}
	if initial.ClientMeta != nil {
		e := *initial.ClientMeta
		e.Endpoints = append([]string(nil), e.Endpoints...)
		v.Configuration.ClientMeta = &e
		v.Metadata = &metadataSession{Label: e.MetaPrefix, Etcd: strings.Join(e.Endpoints, ","), Prefix: e.MetaPrefix}
		v.Metadata.AuthorityID = metadataHistoryID(*v.Metadata)
	}
	if initial.ClientStorage != nil {
		storage := *initial.ClientStorage
		v.Configuration.ClientStorage = &storage
		if v.Metadata != nil {
			v.Metadata.Bucket, v.Metadata.RootPath = storage.Bucket, storage.RootPath
		}
	}
}

func (v *browserSession) detachClient() {
	v.Metadata = nil
	v.Configuration.ClientMeta = nil
	v.Configuration.ClientStorage = nil
}
