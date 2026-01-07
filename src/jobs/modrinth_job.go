package jobs

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/apex/log"
	"github.com/google/uuid"

	"github.com/pyrohost/elytra/src/config"
	"github.com/pyrohost/elytra/src/internal/ufs"
	"github.com/pyrohost/elytra/src/server"
	"github.com/pyrohost/elytra/src/system"
)

const (
	modrinthJobTypeDownload     = "modrinth_download"
	modrinthJobTypeDownloadBulk = "modrinth_download_bulk"
	modrinthJobTypeUpdate       = "modrinth_update"
	modrinthJobTypeRemove       = "modrinth_remove"
	modrinthJobTypeScan         = "modrinth_scan"

	modrinthMaxFileSizeBytes         = int64(100 * 1024 * 1024)
	modrinthItemProgressDownloadBase = 10
	modrinthItemProgressDownloadSpan = 75
	modrinthItemProgressValidate     = 90
	modrinthItemProgressFinalize     = 95
)

var (
	modrinthAllowedDirectories = map[string]struct{}{
		"/mods":    {},
		"/plugins": {},
	}
	modrinthRemovableDirectories = map[string]struct{}{
		"/mods":    {},
		"/plugins": {},
		"/config":  {},
	}
	modrinthAllowedHosts = map[string]struct{}{
		"cdn.modrinth.com":     {},
		"cdn-raw.modrinth.com": {},
	}

	modrinthDownloadClient = newModrinthHTTPClient()

	modrinthBlockedRanges = []*net.IPNet{
		mustParseCIDR("127.0.0.1/8"),
		mustParseCIDR("10.0.0.0/8"),
		mustParseCIDR("172.16.0.0/12"),
		mustParseCIDR("192.168.0.0/16"),
		mustParseCIDR("169.254.0.0/16"),
		mustParseCIDR("::1/128"),
		mustParseCIDR("fe80::/10"),
		mustParseCIDR("fc00::/7"),
	}
)

type ModrinthDownloadJob struct {
	id            string
	serverID      string
	projectID     string
	versionID     string
	fileURL       *url.URL
	fileName      string
	fileHashHex   string
	fileHashBytes []byte
	directory     string
	overwrite     bool
	maxFileSize   int64
	progress      int
	message       string
	serverManager *server.Manager
}

type modrinthDownloadSpec struct {
	projectID     string
	versionID     string
	fileURL       *url.URL
	fileName      string
	fileHashHex   string
	fileHashBytes []byte
	directory     string
	overwrite     bool
	maxFileSize   int64
}

type ModrinthBulkDownloadJob struct {
	id            string
	serverID      string
	downloads     []modrinthDownloadSpec
	progress      int
	message       string
	serverManager *server.Manager
}

type modrinthUpdateSpec struct {
	download    modrinthDownloadSpec
	oldFilePath string
}

type ModrinthUpdateJob struct {
	id             string
	serverID       string
	updates        []modrinthUpdateSpec
	deleteOldFiles bool
	progress       int
	message        string
	serverManager  *server.Manager
}

type ModrinthScanJob struct {
	id            string
	serverID      string
	directories   []string
	progress      int
	message       string
	serverManager *server.Manager
}

type ModrinthRemoveJob struct {
	id            string
	serverID      string
	filePath      string
	progress      int
	message       string
	serverManager *server.Manager
}

func NewModrinthDownloadJob(data map[string]interface{}, serverManager *server.Manager) (Job, error) {
	serverID, err := requireStringField(data, "server_id")
	if err != nil {
		return nil, err
	}

	requestMax, err := optionalInt64Field(data, "max_file_size")
	if err != nil {
		return nil, err
	}

	spec, err := parseModrinthDownloadSpec(data, requestMax, false)
	if err != nil {
		return nil, err
	}

	return &ModrinthDownloadJob{
		id:            uuid.New().String(),
		serverID:      serverID,
		projectID:     spec.projectID,
		versionID:     spec.versionID,
		fileURL:       spec.fileURL,
		fileName:      spec.fileName,
		fileHashHex:   spec.fileHashHex,
		fileHashBytes: spec.fileHashBytes,
		directory:     spec.directory,
		overwrite:     spec.overwrite,
		maxFileSize:   spec.maxFileSize,
		progress:      0,
		message:       "Modrinth download queued",
		serverManager: serverManager,
	}, nil
}

func NewModrinthScanJob(data map[string]interface{}, serverManager *server.Manager) (Job, error) {
	serverID, err := requireStringField(data, "server_id")
	if err != nil {
		return nil, err
	}

	dirs, err := optionalStringSliceField(data, "directories")
	if err != nil {
		return nil, err
	}
	directories, err := normalizeModrinthDirectories(dirs)
	if err != nil {
		return nil, err
	}

	return &ModrinthScanJob{
		id:            uuid.New().String(),
		serverID:      serverID,
		directories:   directories,
		progress:      0,
		message:       "Modrinth scan queued",
		serverManager: serverManager,
	}, nil
}

func NewModrinthBulkDownloadJob(data map[string]interface{}, serverManager *server.Manager) (Job, error) {
	serverID, err := requireStringField(data, "server_id")
	if err != nil {
		return nil, err
	}

	requestMax, err := optionalInt64Field(data, "max_file_size")
	if err != nil {
		return nil, err
	}

	items, err := requireMapSliceField(data, "downloads")
	if err != nil {
		return nil, err
	}

	downloads := make([]modrinthDownloadSpec, 0, len(items))
	for _, item := range items {
		spec, err := parseModrinthDownloadSpec(item, requestMax, false)
		if err != nil {
			return nil, err
		}
		downloads = append(downloads, spec)
	}

	if len(downloads) == 0 {
		return nil, fmt.Errorf("downloads are required")
	}

	return &ModrinthBulkDownloadJob{
		id:            uuid.New().String(),
		serverID:      serverID,
		downloads:     downloads,
		progress:      0,
		message:       "Modrinth bulk download queued",
		serverManager: serverManager,
	}, nil
}

func NewModrinthUpdateJob(data map[string]interface{}, serverManager *server.Manager) (Job, error) {
	serverID, err := requireStringField(data, "server_id")
	if err != nil {
		return nil, err
	}

	requestMax, err := optionalInt64Field(data, "max_file_size")
	if err != nil {
		return nil, err
	}

	items, err := requireMapSliceField(data, "downloads")
	if err != nil {
		return nil, err
	}

	deleteOldFiles, _ := optionalBoolField(data, "delete_old_files")

	updates := make([]modrinthUpdateSpec, 0, len(items))
	for _, item := range items {
		spec, err := parseModrinthDownloadSpec(item, requestMax, true)
		if err != nil {
			return nil, err
		}

		oldPathRaw, err := requireStringField(item, "old_file_path")
		if err != nil {
			return nil, err
		}
		oldPath, err := normalizeModrinthFilePath(oldPathRaw, modrinthAllowedDirectories)
		if err != nil {
			return nil, err
		}

		updates = append(updates, modrinthUpdateSpec{
			download:    spec,
			oldFilePath: oldPath,
		})
	}

	if len(updates) == 0 {
		return nil, fmt.Errorf("downloads are required")
	}

	return &ModrinthUpdateJob{
		id:             uuid.New().String(),
		serverID:       serverID,
		updates:        updates,
		deleteOldFiles: deleteOldFiles,
		progress:       0,
		message:        "Modrinth update queued",
		serverManager:  serverManager,
	}, nil
}

func NewModrinthRemoveJob(data map[string]interface{}, serverManager *server.Manager) (Job, error) {
	serverID, err := requireStringField(data, "server_id")
	if err != nil {
		return nil, err
	}

	filePathRaw, err := requireStringField(data, "file_path")
	if err != nil {
		return nil, err
	}
	filePath, err := normalizeModrinthFilePath(filePathRaw, modrinthRemovableDirectories)
	if err != nil {
		return nil, err
	}

	return &ModrinthRemoveJob{
		id:            uuid.New().String(),
		serverID:      serverID,
		filePath:      filePath,
		progress:      0,
		message:       "Modrinth remove queued",
		serverManager: serverManager,
	}, nil
}

func (j *ModrinthDownloadJob) GetID() string      { return j.id }
func (j *ModrinthDownloadJob) GetType() string    { return modrinthJobTypeDownload }
func (j *ModrinthDownloadJob) GetProgress() int   { return j.progress }
func (j *ModrinthDownloadJob) GetMessage() string { return j.message }
func (j *ModrinthDownloadJob) IsAsync() bool      { return true }

func (j *ModrinthDownloadJob) GetServerID() string           { return j.serverID }
func (j *ModrinthDownloadJob) GetWebSocketEventType() string { return "modrinth.status" }
func (j *ModrinthDownloadJob) GetWebSocketEventData() map[string]interface{} {
	return map[string]interface{}{
		"operation":  "download",
		"project_id": j.projectID,
		"version_id": j.versionID,
		"file_name":  j.fileName,
		"directory":  j.directory,
	}
}

func (j *ModrinthDownloadJob) Validate() error {
	if j.serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	if j.fileURL == nil {
		return fmt.Errorf("file_url is required")
	}
	if j.fileName == "" {
		return fmt.Errorf("file_name is required")
	}
	if j.fileHashHex == "" || len(j.fileHashBytes) == 0 {
		return fmt.Errorf("file_hash is required")
	}
	if j.directory == "" {
		return fmt.Errorf("directory is required")
	}
	return nil
}

func (j *ModrinthDownloadJob) Execute(ctx context.Context, reporter ProgressReporter) (interface{}, error) {
	logger := log.WithFields(log.Fields{
		"job_id":    j.id,
		"server_id": j.serverID,
		"file":      j.fileName,
	})

	if config.Get().Api.DisableRemoteDownload {
		return nil, errors.New("remote downloads are disabled on this node")
	}

	reporter.ReportProgress(5, "Locating server...")
	s, exists := j.serverManager.Get(j.serverID)
	if !exists {
		return nil, errors.New("server not found: " + j.serverID)
	}

	targetPath := path.Join(j.directory, j.fileName)
	if err := s.Filesystem().IsIgnored(targetPath); err != nil {
		return nil, err
	}

	logger.WithField("target", targetPath).Info("starting modrinth download")

	reporter.ReportProgress(10, "Preparing download...")
	result, err := j.downloadToServer(ctx, reporter, s, targetPath, j.maxFileSize)
	if err != nil {
		logger.WithError(err).Error("modrinth download failed")
		return nil, err
	}

	logger.Info("modrinth download completed successfully")
	return result, nil
}

func (j *ModrinthDownloadJob) downloadToServer(ctx context.Context, reporter ProgressReporter, s *server.Server, targetPath string, maxFileSize int64) (map[string]interface{}, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.fileURL.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create download request")
	}
	req.Header.Set("User-Agent", "Elytra Daemon (https://pyrodactyl.dev)")

	resp, err := modrinthDownloadClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to download file")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected download response: %d %s", resp.StatusCode, resp.Status)
	}

	contentLength := resp.ContentLength
	if contentLength < 1 {
		return nil, errors.New("download response missing content length")
	}

	maxSize := resolveModrinthMaxFileSize(maxFileSize)
	if contentLength > maxSize {
		return nil, errors.Errorf("file exceeds maximum size of %s", system.FormatBytes(maxSize))
	}

	tempPath := path.Join(j.directory, fmt.Sprintf(".elytra-modrinth-%s-%s", uuid.New().String(), j.fileName))
	if err := s.Filesystem().IsIgnored(tempPath); err != nil {
		return nil, err
	}

	reporter.ReportProgress(modrinthItemProgressDownloadBase, "Downloading file...")

	hasher := sha512.New()
	progressReader := newModrinthProgressReader(
		io.TeeReader(resp.Body, hasher),
		contentLength,
		j.fileName,
		reporter,
		modrinthItemProgressDownloadBase,
		modrinthItemProgressDownloadSpan,
	)

	if err := s.Filesystem().Write(tempPath, progressReader, contentLength, 0o644); err != nil {
		_ = s.Filesystem().Delete(tempPath)
		return nil, errors.Wrap(err, "failed to write downloaded file")
	}

	if progressReader.BytesRead() < contentLength {
		_ = s.Filesystem().Delete(tempPath)
		return nil, errors.New("download incomplete before expected length")
	}

	actualHash := hasher.Sum(nil)
	if !bytes.Equal(actualHash, j.fileHashBytes) {
		_ = s.Filesystem().Delete(tempPath)
		return nil, errors.New("sha512 hash mismatch for downloaded file")
	}

	reporter.ReportProgress(modrinthItemProgressValidate, "Validating download...")

	if err := j.replaceTargetFile(s, tempPath, targetPath); err != nil {
		_ = s.Filesystem().Delete(tempPath)
		return nil, err
	}

	reporter.ReportProgress(modrinthItemProgressFinalize, "Finalizing download...")

	return map[string]interface{}{
		"file_path": targetPath,
		"file_size": contentLength,
		"file_hash": "sha512:" + hex.EncodeToString(actualHash),
	}, nil
}

func (j *ModrinthDownloadJob) replaceTargetFile(s *server.Server, tempPath, targetPath string) error {
	if st, err := s.Filesystem().Stat(targetPath); err == nil {
		if st.IsDir() {
			return errors.New("target path is a directory")
		}
		if !j.overwrite {
			return errors.New("target file already exists")
		}
		if err := s.Filesystem().Delete(targetPath); err != nil {
			return errors.Wrap(err, "failed to remove existing file")
		}
	} else if !errors.Is(err, ufs.ErrNotExist) {
		return errors.Wrap(err, "failed to check target file")
	}

	if err := s.Filesystem().Rename(tempPath, targetPath); err != nil {
		return errors.Wrap(err, "failed to move downloaded file into place")
	}
	return nil
}

func (j *ModrinthBulkDownloadJob) GetID() string      { return j.id }
func (j *ModrinthBulkDownloadJob) GetType() string    { return modrinthJobTypeDownloadBulk }
func (j *ModrinthBulkDownloadJob) GetProgress() int   { return j.progress }
func (j *ModrinthBulkDownloadJob) GetMessage() string { return j.message }
func (j *ModrinthBulkDownloadJob) IsAsync() bool      { return true }

func (j *ModrinthBulkDownloadJob) GetServerID() string           { return j.serverID }
func (j *ModrinthBulkDownloadJob) GetWebSocketEventType() string { return "modrinth.status" }
func (j *ModrinthBulkDownloadJob) GetWebSocketEventData() map[string]interface{} {
	return map[string]interface{}{
		"operation": "download_bulk",
		"count":     len(j.downloads),
	}
}

func (j *ModrinthBulkDownloadJob) Validate() error {
	if j.serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	if len(j.downloads) == 0 {
		return fmt.Errorf("downloads are required")
	}
	return nil
}

func (j *ModrinthBulkDownloadJob) Execute(ctx context.Context, reporter ProgressReporter) (interface{}, error) {
	logger := log.WithFields(log.Fields{
		"job_id":    j.id,
		"server_id": j.serverID,
		"count":     len(j.downloads),
	})

	if config.Get().Api.DisableRemoteDownload {
		return nil, errors.New("remote downloads are disabled on this node")
	}

	reporter.ReportProgress(5, "Locating server...")
	s, exists := j.serverManager.Get(j.serverID)
	if !exists {
		return nil, errors.New("server not found: " + j.serverID)
	}

	downloaded := make([]map[string]interface{}, 0, len(j.downloads))
	failed := make([]map[string]interface{}, 0)

	for i, download := range j.downloads {
		itemBase, itemSpan := modrinthItemRange(i, len(j.downloads), 10, 80)
		itemReporter := newModrinthScaledReporter(reporter, itemBase, itemSpan)

		itemReporter.ReportProgress(0, fmt.Sprintf("Downloading %d of %d...", i+1, len(j.downloads)))

		result, err := downloadModrinthSpec(ctx, itemReporter, s, download)
		if err != nil {
			logger.WithField("file", download.fileName).WithError(err).Warn("modrinth bulk download failed")
			failed = append(failed, map[string]interface{}{
				"project_id": download.projectID,
				"file_name":  download.fileName,
				"error":      err.Error(),
			})
			continue
		}

		entry := map[string]interface{}{
			"project_id": download.projectID,
			"file_path":  result["file_path"],
			"file_size":  result["file_size"],
		}
		downloaded = append(downloaded, entry)
	}

	reporter.ReportProgress(95, "Finalizing downloads...")

	successful := len(failed) == 0
	return map[string]interface{}{
		"successful":       successful,
		"downloaded_count": len(downloaded),
		"failed_count":     len(failed),
		"downloaded_files": downloaded,
		"failed_downloads": failed,
	}, nil
}

func (j *ModrinthUpdateJob) GetID() string      { return j.id }
func (j *ModrinthUpdateJob) GetType() string    { return modrinthJobTypeUpdate }
func (j *ModrinthUpdateJob) GetProgress() int   { return j.progress }
func (j *ModrinthUpdateJob) GetMessage() string { return j.message }
func (j *ModrinthUpdateJob) IsAsync() bool      { return true }

func (j *ModrinthUpdateJob) GetServerID() string           { return j.serverID }
func (j *ModrinthUpdateJob) GetWebSocketEventType() string { return "modrinth.status" }
func (j *ModrinthUpdateJob) GetWebSocketEventData() map[string]interface{} {
	return map[string]interface{}{
		"operation":        "update",
		"count":            len(j.updates),
		"delete_old_files": j.deleteOldFiles,
	}
}

func (j *ModrinthUpdateJob) Validate() error {
	if j.serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	if len(j.updates) == 0 {
		return fmt.Errorf("downloads are required")
	}
	return nil
}

func (j *ModrinthUpdateJob) Execute(ctx context.Context, reporter ProgressReporter) (interface{}, error) {
	logger := log.WithFields(log.Fields{
		"job_id":    j.id,
		"server_id": j.serverID,
		"count":     len(j.updates),
	})

	if config.Get().Api.DisableRemoteDownload {
		return nil, errors.New("remote downloads are disabled on this node")
	}

	reporter.ReportProgress(5, "Locating server...")
	s, exists := j.serverManager.Get(j.serverID)
	if !exists {
		return nil, errors.New("server not found: " + j.serverID)
	}

	updated := 0
	deleted := make([]string, 0)
	failed := make([]map[string]interface{}, 0)
	deleteFailures := make([]map[string]interface{}, 0)

	for i, update := range j.updates {
		itemBase, itemSpan := modrinthItemRange(i, len(j.updates), 10, 80)
		itemReporter := newModrinthScaledReporter(reporter, itemBase, itemSpan)

		itemReporter.ReportProgress(0, fmt.Sprintf("Updating %d of %d...", i+1, len(j.updates)))

		result, err := downloadModrinthSpec(ctx, itemReporter, s, update.download)
		if err != nil {
			logger.WithField("file", update.download.fileName).WithError(err).Warn("modrinth update download failed")
			failed = append(failed, map[string]interface{}{
				"project_id": update.download.projectID,
				"version_id": update.download.versionID,
				"file_name":  update.download.fileName,
				"error":      err.Error(),
			})
			continue
		}

		updated++

		if j.deleteOldFiles {
			targetPath, _ := result["file_path"].(string)
			if targetPath != "" && targetPath != update.oldFilePath {
				deletedFile, err := deleteModrinthFileIfExists(s, update.oldFilePath)
				if err != nil {
					logger.WithField("old_path", update.oldFilePath).WithError(err).Warn("failed to remove old mod file")
					deleteFailures = append(deleteFailures, map[string]interface{}{
						"file_path": update.oldFilePath,
						"error":     err.Error(),
					})
				} else if deletedFile {
					deleted = append(deleted, update.oldFilePath)
				}
			}
		}
	}

	reporter.ReportProgress(95, "Finalizing updates...")

	successful := len(failed) == 0 && len(deleteFailures) == 0
	return map[string]interface{}{
		"successful":        successful,
		"updated_count":     updated,
		"failed_count":      len(failed),
		"failed_updates":    failed,
		"deleted_old_files": deleted,
		"delete_failures":   deleteFailures,
	}, nil
}

func (j *ModrinthRemoveJob) GetID() string      { return j.id }
func (j *ModrinthRemoveJob) GetType() string    { return modrinthJobTypeRemove }
func (j *ModrinthRemoveJob) GetProgress() int   { return j.progress }
func (j *ModrinthRemoveJob) GetMessage() string { return j.message }

func (j *ModrinthRemoveJob) GetServerID() string           { return j.serverID }
func (j *ModrinthRemoveJob) GetWebSocketEventType() string { return "modrinth.status" }
func (j *ModrinthRemoveJob) GetWebSocketEventData() map[string]interface{} {
	return map[string]interface{}{
		"operation": "remove",
		"file_path": j.filePath,
	}
}

func (j *ModrinthRemoveJob) Validate() error {
	if j.serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	if j.filePath == "" {
		return fmt.Errorf("file_path is required")
	}
	return nil
}

func (j *ModrinthRemoveJob) Execute(ctx context.Context, reporter ProgressReporter) (interface{}, error) {
	logger := log.WithFields(log.Fields{
		"job_id":    j.id,
		"server_id": j.serverID,
		"file":      j.filePath,
	})

	reporter.ReportProgress(5, "Locating server...")
	s, exists := j.serverManager.Get(j.serverID)
	if !exists {
		return nil, errors.New("server not found: " + j.serverID)
	}

	reporter.ReportProgress(50, "Removing file...")
	deleted, err := deleteModrinthFileIfExists(s, j.filePath)
	if err != nil {
		logger.WithError(err).Error("modrinth remove failed")
		return nil, err
	}

	message := "File removed"
	if !deleted {
		message = "File already removed"
	}
	reporter.ReportProgress(90, message)

	return map[string]interface{}{
		"deleted_path": j.filePath,
		"deleted":      deleted,
	}, nil
}

func (j *ModrinthScanJob) GetID() string      { return j.id }
func (j *ModrinthScanJob) GetType() string    { return modrinthJobTypeScan }
func (j *ModrinthScanJob) GetProgress() int   { return j.progress }
func (j *ModrinthScanJob) GetMessage() string { return j.message }

func (j *ModrinthScanJob) GetServerID() string           { return j.serverID }
func (j *ModrinthScanJob) GetWebSocketEventType() string { return "modrinth.status" }
func (j *ModrinthScanJob) GetWebSocketEventData() map[string]interface{} {
	return map[string]interface{}{
		"operation":   "scan",
		"directories": j.directories,
	}
}

func (j *ModrinthScanJob) Validate() error {
	if j.serverID == "" {
		return fmt.Errorf("server_id is required")
	}
	if len(j.directories) == 0 {
		return fmt.Errorf("directories are required")
	}
	return nil
}

func (j *ModrinthScanJob) Execute(ctx context.Context, reporter ProgressReporter) (interface{}, error) {
	logger := log.WithFields(log.Fields{
		"job_id":    j.id,
		"server_id": j.serverID,
	})

	reporter.ReportProgress(5, "Locating server...")
	s, exists := j.serverManager.Get(j.serverID)
	if !exists {
		return nil, errors.New("server not found: " + j.serverID)
	}

	reporter.ReportProgress(10, "Scanning mod directories...")
	entries, err := j.collectJarEntries(s)
	if err != nil {
		logger.WithError(err).Error("modrinth scan failed")
		return nil, err
	}

	if len(entries) == 0 {
		reporter.ReportProgress(90, "No mods detected")
		return map[string]interface{}{
			"successful": true,
			"job_type":   modrinthJobTypeScan,
			"files":      []map[string]interface{}{},
		}, nil
	}

	reporter.ReportProgress(20, fmt.Sprintf("Found %d files, hashing...", len(entries)))

	files := make([]map[string]interface{}, 0, len(entries))
	for i, entry := range entries {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		sha512Hex, err := hashFileSHA512(s, entry.path)
		if err != nil {
			return nil, err
		}

		// Calculate additional hashes for cross-platform matching
		sha1Hex, err := hashFileSHA1(s, entry.path)
		if err != nil {
			return nil, err
		}

		murmur2, err := hashFileMurmur2(s, entry.path)
		if err != nil {
			return nil, err
		}

		progress := 20 + ((i + 1) * 60 / len(entries))
		reporter.ReportProgress(progress, fmt.Sprintf("Hashed %s (%d/%d)", entry.name, i+1, len(entries)))

		files = append(files, map[string]interface{}{
			"path":     entry.path,
			"name":     entry.name,
			"size":     entry.size,
			"hash":     "sha512:" + sha512Hex,
			"hashes": map[string]interface{}{
				"sha1":    sha1Hex,
				"sha512":  sha512Hex,
				"murmur2": murmur2,
			},
			"modified": entry.modified,
		})
	}

	reporter.ReportProgress(95, "Finalizing scan results...")

	return map[string]interface{}{
		"successful": true,
		"job_type":   modrinthJobTypeScan,
		"files":      files,
	}, nil
}

type modrinthJarEntry struct {
	path     string
	name     string
	size     int64
	modified string
}

func (j *ModrinthScanJob) collectJarEntries(s *server.Server) ([]modrinthJarEntry, error) {
	var entries []modrinthJarEntry
	fs := s.Filesystem().UnixFS()
	for _, dir := range j.directories {
		err := ufs.WalkDir(fs, dir, func(p string, d ufs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, ufs.ErrNotExist) {
					return ufs.SkipDir
				}
				return err
			}
			if d.IsDir() {
				return nil
			}
			if !d.Type().IsRegular() {
				return nil
			}
			if !strings.HasSuffix(strings.ToLower(d.Name()), ".jar") {
				return nil
			}
			// Use Stat with the full path instead of d.Info() to avoid
			// "bad file descriptor" errors. The DirEntry's Info() method
			// uses a cached directory fd that may be closed by the time
			// we call it.
			info, err := fs.Stat(p)
			if err != nil {
				return err
			}
			entries = append(entries, modrinthJarEntry{
				path:     p,
				name:     info.Name(),
				size:     info.Size(),
				modified: info.ModTime().UTC().Format(time.RFC3339),
			})
			return nil
		})
		if err != nil && !errors.Is(err, ufs.SkipDir) {
			return nil, errors.Wrap(err, "failed to scan mod directory")
		}
	}
	return entries, nil
}

type modrinthProgressReader struct {
	reader      io.Reader
	total       int64
	read        int64
	fileName    string
	reporter    ProgressReporter
	lastPercent int
	base        int
	span        int
}

func newModrinthProgressReader(reader io.Reader, total int64, fileName string, reporter ProgressReporter, base, span int) *modrinthProgressReader {
	return &modrinthProgressReader{
		reader:   reader,
		total:    total,
		fileName: fileName,
		reporter: reporter,
		base:     base,
		span:     span,
	}
}

func (p *modrinthProgressReader) Read(b []byte) (int, error) {
	n, err := p.reader.Read(b)
	if n > 0 {
		p.read += int64(n)
		p.report()
	}
	return n, err
}

func (p *modrinthProgressReader) BytesRead() int64 {
	return p.read
}

func (p *modrinthProgressReader) report() {
	if p.total <= 0 {
		return
	}
	percent := int((p.read * 100) / p.total)
	if percent <= p.lastPercent {
		return
	}
	p.lastPercent = percent

	progress := p.base + (percent*p.span)/100
	if progress > p.base+p.span {
		progress = p.base + p.span
	}

	message := fmt.Sprintf(
		"Downloading %s (%s/%s)",
		p.fileName,
		system.FormatBytes(p.read),
		system.FormatBytes(p.total),
	)

	p.reporter.ReportProgress(progress, message)
}

type modrinthScaledReporter struct {
	base     int
	span     int
	reporter ProgressReporter
}

func newModrinthScaledReporter(reporter ProgressReporter, base, span int) *modrinthScaledReporter {
	return &modrinthScaledReporter{
		base:     base,
		span:     span,
		reporter: reporter,
	}
}

func (r *modrinthScaledReporter) ReportProgress(progress int, message string) {
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}
	scaled := r.base + (progress*r.span)/100
	if scaled > 100 {
		scaled = 100
	}
	r.reporter.ReportProgress(scaled, message)
}

func (r *modrinthScaledReporter) ReportStatus(status Status, message string) {
	r.reporter.ReportStatus(status, message)
}

func modrinthItemRange(index, total, base, span int) (int, int) {
	if total <= 0 {
		return base, span
	}
	start := base + (index*span)/total
	end := base + ((index+1)*span)/total
	if end < start {
		end = start
	}
	return start, end - start
}

func downloadModrinthSpec(ctx context.Context, reporter ProgressReporter, s *server.Server, spec modrinthDownloadSpec) (map[string]interface{}, error) {
	targetPath := path.Join(spec.directory, spec.fileName)
	if err := s.Filesystem().IsIgnored(targetPath); err != nil {
		return nil, err
	}

	job := &ModrinthDownloadJob{
		fileURL:       spec.fileURL,
		fileName:      spec.fileName,
		fileHashHex:   spec.fileHashHex,
		fileHashBytes: spec.fileHashBytes,
		directory:     spec.directory,
		overwrite:     spec.overwrite,
	}

	return job.downloadToServer(ctx, reporter, s, targetPath, spec.maxFileSize)
}

func parseModrinthURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || !u.IsAbs() {
		return nil, fmt.Errorf("invalid file_url")
	}
	if u.Scheme != "https" {
		return nil, fmt.Errorf("file_url must use https")
	}
	host := strings.ToLower(u.Hostname())
	if _, ok := modrinthAllowedHosts[host]; !ok {
		return nil, fmt.Errorf("file_url host is not allowed")
	}
	return u, nil
}

func normalizeModrinthDirectory(dir string) (string, error) {
	cleaned := path.Clean("/" + strings.TrimSpace(dir))
	if cleaned == "." || cleaned == "/" {
		return "", fmt.Errorf("directory is required")
	}
	if _, ok := modrinthAllowedDirectories[cleaned]; !ok {
		return "", fmt.Errorf("directory must be /mods or /plugins")
	}
	return cleaned, nil
}

func normalizeModrinthDirectories(dirs []string) ([]string, error) {
	if len(dirs) == 0 {
		return []string{"/mods", "/plugins"}, nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		normalized, err := normalizeModrinthDirectory(dir)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out, nil
}

func normalizeModrinthFilePath(p string, allowed map[string]struct{}) (string, error) {
	cleaned := path.Clean("/" + strings.TrimSpace(p))
	if cleaned == "." || cleaned == "/" {
		return "", fmt.Errorf("file_path is required")
	}
	for dir := range allowed {
		if cleaned == dir || strings.HasPrefix(cleaned, dir+"/") {
			return cleaned, nil
		}
	}
	return "", fmt.Errorf("file_path must be within allowed directories")
}

func normalizeModrinthFileName(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", fmt.Errorf("file_name is required")
	}
	if strings.Contains(trimmed, "/") || strings.Contains(trimmed, "\\") {
		return "", fmt.Errorf("file_name must not include path separators")
	}
	if trimmed == "." || trimmed == ".." {
		return "", fmt.Errorf("file_name must be a file")
	}
	return trimmed, nil
}

func parseModrinthSHA512(raw string) ([]byte, string, error) {
	cleaned := strings.TrimSpace(raw)
	if cleaned == "" {
		return nil, "", fmt.Errorf("file_hash is required")
	}
	if len(cleaned) >= len("sha512:") && strings.EqualFold(cleaned[:len("sha512:")], "sha512:") {
		cleaned = cleaned[len("sha512:"):]
	}
	if cleaned == "" {
		return nil, "", fmt.Errorf("file_hash is required")
	}

	lower := strings.ToLower(cleaned)
	if len(lower) == sha512.Size*2 && isHex(lower) {
		b, err := hex.DecodeString(lower)
		if err != nil {
			return nil, "", err
		}
		return b, lower, nil
	}

	if b, err := base64.StdEncoding.DecodeString(cleaned); err == nil && len(b) == sha512.Size {
		return b, hex.EncodeToString(b), nil
	}
	if b, err := base64.RawStdEncoding.DecodeString(cleaned); err == nil && len(b) == sha512.Size {
		return b, hex.EncodeToString(b), nil
	}
	return nil, "", fmt.Errorf("file_hash must be a sha512 hex or base64 string")
}

func resolveModrinthMaxFileSize(requestMax int64) int64 {
	if requestMax > 0 {
		return requestMax
	}
	return modrinthMaxFileSizeBytes
}

func parseModrinthDownloadSpec(data map[string]interface{}, requestMax int64, forceOverwrite bool) (modrinthDownloadSpec, error) {
	projectID, _ := optionalStringField(data, "project_id")
	versionID, _ := optionalStringField(data, "version_id")

	fileURLRaw, err := requireStringField(data, "file_url")
	if err != nil {
		return modrinthDownloadSpec{}, err
	}
	fileNameRaw, err := requireStringField(data, "file_name")
	if err != nil {
		return modrinthDownloadSpec{}, err
	}
	fileHashRaw, err := requireStringField(data, "file_hash")
	if err != nil {
		return modrinthDownloadSpec{}, err
	}
	directoryRaw, err := requireStringField(data, "directory")
	if err != nil {
		return modrinthDownloadSpec{}, err
	}

	fileURL, err := parseModrinthURL(fileURLRaw)
	if err != nil {
		return modrinthDownloadSpec{}, err
	}

	fileName, err := normalizeModrinthFileName(fileNameRaw)
	if err != nil {
		return modrinthDownloadSpec{}, err
	}

	directory, err := normalizeModrinthDirectory(directoryRaw)
	if err != nil {
		return modrinthDownloadSpec{}, err
	}

	hashBytes, hashHex, err := parseModrinthSHA512(fileHashRaw)
	if err != nil {
		return modrinthDownloadSpec{}, err
	}

	overwrite, _ := optionalBoolField(data, "overwrite")
	if forceOverwrite {
		overwrite = true
	}

	itemMax, err := optionalInt64Field(data, "max_file_size")
	if err != nil {
		return modrinthDownloadSpec{}, err
	}
	if itemMax <= 0 {
		itemMax = requestMax
	}

	return modrinthDownloadSpec{
		projectID:     projectID,
		versionID:     versionID,
		fileURL:       fileURL,
		fileName:      fileName,
		fileHashHex:   hashHex,
		fileHashBytes: hashBytes,
		directory:     directory,
		overwrite:     overwrite,
		maxFileSize:   resolveModrinthMaxFileSize(itemMax),
	}, nil
}

func isHex(s string) bool {
	for _, r := range s {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		return false
	}
	return true
}

func hashFileSHA512(s *server.Server, filePath string) (string, error) {
	f, err := s.Filesystem().UnixFS().Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hasher := sha512.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func hashFileSHA1(s *server.Server, filePath string) (string, error) {
	f, err := s.Filesystem().UnixFS().Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hasher := sha1.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func deleteModrinthFileIfExists(s *server.Server, filePath string) (bool, error) {
	if err := s.Filesystem().IsIgnored(filePath); err != nil {
		return false, err
	}
	st, err := s.Filesystem().Stat(filePath)
	if err != nil {
		if errors.Is(err, ufs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if st.IsDir() {
		return false, errors.New("target path is a directory")
	}
	if err := s.Filesystem().Delete(filePath); err != nil {
		return false, err
	}
	return true, nil
}

func optionalStringField(data map[string]interface{}, key string) (string, bool) {
	v, ok := data[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return "", false
	}
	return strings.TrimSpace(s), true
}

func optionalInt64Field(data map[string]interface{}, key string) (int64, error) {
	v, ok := data[key]
	if !ok || v == nil {
		return 0, nil
	}
	switch val := v.(type) {
	case int:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case uint:
		return int64(val), nil
	case uint8:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint64:
		if val > uint64(^uint64(0)>>1) {
			return 0, fmt.Errorf("%s is out of range", key)
		}
		return int64(val), nil
	case float32:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case json.Number:
		parsed, err := val.Int64()
		if err != nil {
			return 0, fmt.Errorf("%s must be a number", key)
		}
		return parsed, nil
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%s must be a number", key)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("%s must be a number", key)
	}
}

func requireStringField(data map[string]interface{}, key string) (string, error) {
	v, ok := data[key]
	if !ok {
		return "", fmt.Errorf("%s is required", key)
	}
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return strings.TrimSpace(s), nil
}

func requireMapSliceField(data map[string]interface{}, key string) ([]map[string]interface{}, error) {
	v, ok := data[key]
	if !ok || v == nil {
		return nil, fmt.Errorf("%s is required", key)
	}
	switch raw := v.(type) {
	case []map[string]interface{}:
		if len(raw) == 0 {
			return nil, fmt.Errorf("%s is required", key)
		}
		return raw, nil
	case []interface{}:
		if len(raw) == 0 {
			return nil, fmt.Errorf("%s is required", key)
		}
		out := make([]map[string]interface{}, 0, len(raw))
		for _, item := range raw {
			m, ok := item.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("%s must be a list of objects", key)
			}
			out = append(out, m)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%s must be a list of objects", key)
	}
}

func optionalBoolField(data map[string]interface{}, key string) (bool, bool) {
	v, ok := data[key]
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}

func optionalStringSliceField(data map[string]interface{}, key string) ([]string, error) {
	v, ok := data[key]
	if !ok {
		return nil, nil
	}
	if v == nil {
		return nil, nil
	}
	switch raw := v.(type) {
	case []string:
		return raw, nil
	case []interface{}:
		out := make([]string, 0, len(raw))
		for _, item := range raw {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, strings.TrimSpace(s))
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%s must be a list of strings", key)
	}
}

func newModrinthHTTPClient() *http.Client {
	dialer := &net.Dialer{}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		ipStr, _, err := net.SplitHostPort(c.RemoteAddr().String())
		if err != nil {
			return c, err
		}
		ip := net.ParseIP(ipStr)
		if ip == nil {
			return c, errors.New("invalid IP address")
		}
		if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() {
			return c, errors.New("internal IP resolution blocked")
		}
		for _, block := range modrinthBlockedRanges {
			if block.Contains(ip) {
				return c, errors.New("internal IP resolution blocked")
			}
		}
		return c, nil
	}

	return &http.Client{
		Timeout:   12 * time.Hour,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func mustParseCIDR(ip string) *net.IPNet {
	_, block, err := net.ParseCIDR(ip)
	if err != nil {
		panic(fmt.Errorf("modrinth: failed to parse CIDR: %s", err))
	}
	return block
}
