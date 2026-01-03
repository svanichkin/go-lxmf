package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/svanichkin/configobj"
	"github.com/svanichkin/go-lxmf/lxmf"
	"github.com/svanichkin/go-reticulum/rns"
)

const (
	deferredJobsDelay = 10 * time.Second
	jobsInterval      = 5 * time.Second
	defaultConfigFile = `# This is an example LXM Daemon config file.
# You should probably edit it to suit your
# intended usage.

[propagation]

# Whether to enable propagation node

enable_node = no

# You can specify identity hashes for remotes
# that are allowed to control and query status
# for this propagation node.

# control_allowed = 7d7e542829b40f32364499b27438dba8, 437229f8e29598b2282b88bad5e44698

# An optional name for this node, included
# in announces.

# node_name = Anonymous Propagation Node

# Automatic announce interval in minutes.
# 6 hours by default.

announce_interval = 360

# Whether to announce when the node starts.

announce_at_start = yes

# Wheter to automatically peer with other
# propagation nodes on the network.

autopeer = yes

# The maximum peering depth (in hops) for
# automatically peered nodes.

autopeer_maxdepth = 4

# The maximum amount of storage to use for
# the LXMF Propagation Node message store,
# specified in megabytes. When this limit
# is reached, LXMF will periodically remove
# messages in its message store. By default,
# LXMF prioritises keeping messages that are
# new and small. Large and old messages will
# be removed first. This setting is optional
# and defaults to 500 megabytes.

# message_storage_limit = 500

# The maximum accepted transfer size per in-
# coming propagation message, in kilobytes.
# This sets the upper limit for the size of
# single messages accepted onto this node.

# propagation_message_max_accepted_size = 256

# The maximum accepted transfer size per in-
# coming propagation node sync.
#
# If a node wants to propagate a larger number
# of messages to this node, than what can fit
# within this limit, it will prioritise sending
# the smallest messages first, and try again
# with any remaining messages at a later point.

# propagation_sync_max_accepted_size = 10240

# You can configure the target stamp cost
# required to deliver messages via this node.

# propagation_stamp_cost_target = 16

# If set higher than 0, the stamp cost flexi-
# bility option will make this node accept
# messages with a lower stamp cost than the
# target from other propagation nodes (but
# not from peers directly). This allows the
# network to gradually adjust stamp cost.

# propagation_stamp_cost_flexibility = 3

# The peering_cost option configures the target
# value required for a remote node to peer with
# and deliver messages to this node.

# peering_cost = 18

# You can configure the maximum peering cost
# of remote nodes that this node will peer with.
# Setting this to a higher number will allow
# this node to peer with other nodes requiring
# a higher peering key value, but will require
# more computation time during initial peering
# when generating the peering key.

# remote_peering_cost_max = 26

# You can tell the LXMF message router to
# prioritise storage for one or more
# destinations. If the message store reaches
# the specified limit, LXMF will prioritise
# keeping messages for destinations specified
# with this option. This setting is optional,
# and generally you do not need to use it.

# prioritise_destinations = 41d20c727598a3fbbdf9106133a3a0ed, d924b81822ca24e68e2effea99bcb8cf

# You can configure the maximum number of other
# propagation nodes that this node will peer
# with automatically. The default is 20.

# max_peers = 20

# You can configure a list of static propagation
# node peers, that this node will always be
# peered with, by specifying a list of
# destination hashes.

# static_peers = e17f833c4ddf8890dd3a79a6fea8161d, 5a2d0029b6e5ec87020abaea0d746da4

# You can configure the propagation node to
# only accept incoming propagation messages
# from configured static peers.

# from_static_only = True

# By default, any destination is allowed to
# connect and download messages, but you can
# optionally restrict this. If you enable
# authentication, you must provide a list of
# allowed identity hashes in the a file named
# "allowed" in the lxmd config directory.

auth_required = no


[lxmf]

# The LXM Daemon will create an LXMF destination
# that it can receive messages on. This option sets
# the announced display name for this destination.

display_name = Anonymous Peer

# It is possible to announce the internal LXMF
# destination when the LXM Daemon starts up.

announce_at_start = no

# You can also announce the delivery destination
# at a specified interval. This is not enabled by
# default.

# announce_interval = 360

# The maximum accepted unpacked size for mes-
# sages received directly from other peers,
# specified in kilobytes. Messages larger than
# this will be rejected before the transfer
# begins.

delivery_transfer_max_accepted_size = 1000

# You can configure an external program to be run
# every time a message is received. The program
# will receive as an argument the full path to the
# message saved as a file. The example below will
# simply result in the message getting deleted as
# soon as it has been received.

# on_inbound = rm


[logging]
# Valid log levels are 0 through 7:
#   0: Log only critical information
#   1: Log errors and lower log levels
#   2: Log warnings and lower log levels
#   3: Log notices and lower (this is the default)
#   4: Log info and lower (this is the default)
#   5: Verbose logging
#   6: Debug logging
#   7: Extreme logging

loglevel = 4
`
)

type activeConfiguration struct {
	DisplayName                     string
	PeerAnnounceAtStart             bool
	PeerAnnounceInterval            time.Duration
	DeliveryTransferMaxAcceptedSize int
	OnInbound                       string

	EnablePropagationNode              bool
	NodeName                           string
	AuthRequired                       bool
	NodeAnnounceAtStart                bool
	AutoPeer                           bool
	AutoPeerMaxDepth                   int
	NodeAnnounceInterval               time.Duration
	MessageStorageLimitMB              int
	PropagationTransferMaxAcceptedSize int
	PropagationMessageMaxAcceptedSize  int
	PropagationSyncMaxAcceptedSize     int
	PropagationStampCostTarget         int
	PropagationStampCostFlexibility    int
	PeeringCost                        int
	RemotePeeringCostMax               int
	PrioritisedDestinations            []string
	ControlAllowedIdentities           []string
	StaticPeers                        [][]byte
	FromStaticOnly                     bool
	MaxPeers                           int

	IgnoredLXMFDestinations [][]byte
	AllowedIdentities       [][]byte
}

var (
	configPath   string
	ignoredPath  string
	allowedPath  string
	identityPath string
	storageDir   string
	messagesDir  string

	targetLogLevel = 3

	lxmdConfig   *configobj.Config
	activeConfig = activeConfiguration{}

	identity        *rns.Identity
	messageRouter   *lxmf.LXMRouter
	lxmfDestination *rns.Destination

	lastPeerAnnounce time.Time
	lastNodeAnnounce time.Time
)

func getSection(name string) *configobj.Section {
	if lxmdConfig == nil {
		return nil
	}
	return lxmdConfig.Section(name)
}

func stringKey(section, key, def string) string {
	sec := getSection(section)
	if sec == nil {
		return def
	}
	if value, ok := sec.Get(key); ok && value != "" {
		return value
	}
	return def
}

func boolKey(section, key string, def bool) bool {
	sec := getSection(section)
	if sec == nil {
		return def
	}
	if value, err := sec.AsBool(key); err == nil {
		return value
	}
	return def
}

func intKey(section, key string, def int) int {
	sec := getSection(section)
	if sec == nil {
		return def
	}
	if value, err := sec.AsInt(key); err == nil {
		return value
	}
	return def
}

func floatKey(section, key string, def float64) float64 {
	sec := getSection(section)
	if sec == nil {
		return def
	}
	if value, err := sec.AsFloat(key); err == nil {
		return value
	}
	return def
}

func parseCommaList(value string) []string {
	var out []string
	for _, part := range strings.Split(value, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func loadHashList(path string) [][]byte {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()

	var hashes [][]byte
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if decoded, err := hex.DecodeString(line); err == nil {
			hashes = append(hashes, decoded)
		}
	}
	return hashes
}

func applyConfig() error {
	if lxmdConfig == nil {
		return errors.New("configuration missing")
	}

	activeConfig.DisplayName = stringKey("lxmf", "display_name", "Anonymous Peer")
	activeConfig.PeerAnnounceAtStart = boolKey("lxmf", "announce_at_start", false)
	activeConfig.PeerAnnounceInterval = time.Duration(intKey("lxmf", "announce_interval", 0)) * time.Minute
	activeConfig.DeliveryTransferMaxAcceptedSize = int(floatKey("lxmf", "delivery_transfer_max_accepted_size", 1000))
	activeConfig.OnInbound = stringKey("lxmf", "on_inbound", "")

	activeConfig.EnablePropagationNode = boolKey("propagation", "enable_node", false)
	activeConfig.NodeName = stringKey("propagation", "node_name", "")
	activeConfig.AuthRequired = boolKey("propagation", "auth_required", false)
	activeConfig.NodeAnnounceAtStart = boolKey("propagation", "announce_at_start", false)
	activeConfig.AutoPeer = boolKey("propagation", "autopeer", true)
	activeConfig.AutoPeerMaxDepth = intKey("propagation", "autopeer_maxdepth", 4)
	activeConfig.NodeAnnounceInterval = time.Duration(intKey("propagation", "announce_interval", 0)) * time.Minute
	activeConfig.MessageStorageLimitMB = int(floatKey("propagation", "message_storage_limit", 500))
	activeConfig.PropagationTransferMaxAcceptedSize = int(floatKey("propagation", "propagation_transfer_max_accepted_size", 256))
	activeConfig.PropagationMessageMaxAcceptedSize = int(floatKey("propagation", "propagation_message_max_accepted_size", 256))
	activeConfig.PropagationSyncMaxAcceptedSize = int(floatKey("propagation", "propagation_sync_max_accepted_size", 256*40))
	activeConfig.PropagationStampCostTarget = intKey("propagation", "propagation_stamp_cost_target", 16)
	activeConfig.PropagationStampCostFlexibility = intKey("propagation", "propagation_stamp_cost_flexibility", 3)
	activeConfig.PeeringCost = intKey("propagation", "peering_cost", 18)
	activeConfig.RemotePeeringCostMax = intKey("propagation", "remote_peering_cost_max", 26)
	activeConfig.PrioritisedDestinations = parseCommaList(stringKey("propagation", "prioritise_destinations", ""))
	activeConfig.ControlAllowedIdentities = parseCommaList(stringKey("propagation", "control_allowed", ""))
	activeConfig.StaticPeers = [][]byte{}
	for _, peer := range parseCommaList(stringKey("propagation", "static_peers", "")) {
		if decoded, err := hex.DecodeString(peer); err == nil {
			activeConfig.StaticPeers = append(activeConfig.StaticPeers, decoded)
		}
	}
	activeConfig.FromStaticOnly = boolKey("propagation", "from_static_only", false)
	activeConfig.MaxPeers = intKey("propagation", "max_peers", 20)

	activeConfig.IgnoredLXMFDestinations = loadHashList(ignoredPath)
	activeConfig.AllowedIdentities = loadHashList(allowedPath)

	targetLogLevel = intKey("logging", "loglevel", 4)

	return nil
}

func lxmfDelivery(msg *lxmf.LXMessage) {
	if msg == nil || messagesDir == "" {
		return
	}
	written, err := msg.WriteToDirectory(messagesDir)
	if err != nil {
		rns.Log("Error saving inbound LXMF message: "+err.Error(), rns.LOG_ERROR)
		return
	}
	rns.Log("Received "+msg.String()+" written to "+written, rns.LOG_DEBUG)
	if activeConfig.OnInbound != "" {
		cmd := exec.Command("sh", "-c", fmt.Sprintf("%s %q", activeConfig.OnInbound, written))
		if err := cmd.Run(); err != nil {
			rns.Log("Inbound action failed: "+err.Error(), rns.LOG_ERROR)
		}
	} else {
		rns.Log("No action defined for inbound messages, ignoring", rns.LOG_DEBUG)
	}
}

func programSetup(configDir, rnsConfigDir string, runPN bool, onInbound string, verbosity, quietness int, service bool) {
	if onInbound != "" {
		activeConfig.OnInbound = onInbound
	}

	if configDir == "" {
		if dirExists("/etc/lxmd") && fileExists("/etc/lxmd/config") {
			configDir = "/etc/lxmd"
		} else if userdir, err := os.UserHomeDir(); err == nil {
			candidate := filepath.Join(userdir, ".config/lxmd")
			if dirExists(candidate) && fileExists(filepath.Join(candidate, "config")) {
				configDir = candidate
			} else {
				configDir = filepath.Join(userdir, ".lxmd")
			}
		}
	}

	configPath = filepath.Join(configDir, "config")
	ignoredPath = filepath.Join(configDir, "ignored")
	allowedPath = filepath.Join(configDir, "allowed")
	identityPath = filepath.Join(configDir, "identity")
	storageDir = filepath.Join(configDir, "storage")
	messagesDir = filepath.Join(storageDir, "messages")

	if err := os.MkdirAll(messagesDir, 0o755); err != nil {
		rns.Log("Could not create storage directories: "+err.Error(), rns.LOG_ERROR)
		os.Exit(1)
	}

	if !fileExists(configPath) {
		rns.Log("Could not load config file, creating default configuration file...", rns.LOG_WARNING)
		if err := os.WriteFile(configPath, []byte(defaultConfigFile), 0o644); err != nil {
			rns.Log("Failed to create default config: "+err.Error(), rns.LOG_ERROR)
			os.Exit(1)
		}
		rns.Log("Default config file created. Make any necessary changes in "+configPath+" and restart lxmd if needed.", rns.LOG_INFO)
	}

	var err error
	lxmdConfig, err = configobj.Load(configPath)
	if err != nil {
		rns.Log("Could not parse configuration: "+err.Error(), rns.LOG_ERROR)
		os.Exit(1)
	}

	if err := applyConfig(); err != nil {
		rns.Log("Error applying configuration: "+err.Error(), rns.LOG_ERROR)
		os.Exit(1)
	}

	targetLogLevel = targetLogLevel + verbosity - quietness

	var logDest any = rns.LOG_STDOUT
	if service {
		logDest = rns.LOG_FILE
	}

	if _, err := rns.NewReticulum(ptrOrNil(rnsConfigDir), &targetLogLevel, logDest, nil, false, nil); err != nil {
		rns.Log("Could not start Reticulum: "+err.Error(), rns.LOG_ERROR)
		os.Exit(1)
	}

	if fileExists(identityPath) {
		identity, err = rns.IdentityFromFile(identityPath)
		if err != nil {
			rns.Log("Could not load identity: "+err.Error(), rns.LOG_ERROR)
			os.Exit(1)
		}
	} else {
		identity, err = rns.NewIdentity()
		if err != nil {
			rns.Log("Could not create identity: "+err.Error(), rns.LOG_ERROR)
			os.Exit(1)
		}
		if err := identity.Save(identityPath); err != nil {
			rns.Log("Could not save identity: "+err.Error(), rns.LOG_ERROR)
			os.Exit(1)
		}
	}

	messageRouter, err = lxmf.NewLXMRouter(identity, storageDir)
	if err != nil {
		rns.Log("Could not start LXMF router: "+err.Error(), rns.LOG_ERROR)
		os.Exit(1)
	}

	messageRouter.AutoPeer = activeConfig.AutoPeer
	messageRouter.AutoPeerMaxDepth = activeConfig.AutoPeerMaxDepth
	if activeConfig.PeerAnnounceInterval > 0 {
		lastPeerAnnounce = time.Now().Add(-activeConfig.PeerAnnounceInterval)
	}
	if activeConfig.NodeAnnounceInterval > 0 {
		lastNodeAnnounce = time.Now().Add(-activeConfig.NodeAnnounceInterval)
	}
	messageRouter.PropagationPerTransferLimit = activeConfig.PropagationTransferMaxAcceptedSize
	messageRouter.PropagationPerSyncLimit = activeConfig.PropagationSyncMaxAcceptedSize
	messageRouter.DeliveryPerTransferLimit = activeConfig.DeliveryTransferMaxAcceptedSize
	messageRouter.PropagationStampCost = activeConfig.PropagationStampCostTarget
	messageRouter.PropagationStampCostFlexibility = activeConfig.PropagationStampCostFlexibility
	messageRouter.PeeringCost = activeConfig.PeeringCost
	messageRouter.MaxPeeringCost = activeConfig.RemotePeeringCostMax
	if activeConfig.MaxPeers > 0 {
		messageRouter.MaxPeers = activeConfig.MaxPeers
	}
	messageRouter.StaticPeers = activeConfig.StaticPeers
	messageRouter.FromStaticOnly = activeConfig.FromStaticOnly
	messageRouter.Name = activeConfig.NodeName
	messageRouter.RegisterDeliveryCallback(lxmfDelivery)

	for _, ignored := range activeConfig.IgnoredLXMFDestinations {
		if len(ignored) == rns.ReticulumTruncatedHashLength/8 {
			messageRouter.IgnoreDestination(ignored)
		}
	}

	lxmfDestination = messageRouter.RegisterDeliveryIdentity(identity, activeConfig.DisplayName, nil)
	_ = rns.IdentityRemember(nil, lxmfDestination.Hash(), identity.GetPublicKey(), nil)
	if activeConfig.AuthRequired {
		messageRouter.SetAuthentication(true)
		for _, allowed := range activeConfig.AllowedIdentities {
			if len(allowed) == rns.ReticulumTruncatedHashLength/8 {
				messageRouter.Allow(allowed)
			}
		}
	}

	if activeConfig.MessageStorageLimitMB > 0 {
		_ = messageRouter.SetMessageStorageLimit(0, activeConfig.MessageStorageLimitMB, 0)
	}
	for _, dest := range activeConfig.PrioritisedDestinations {
		if decoded, err := hex.DecodeString(dest); err == nil && len(decoded) == rns.ReticulumTruncatedHashLength/8 {
			messageRouter.Prioritise(decoded)
		}
	}

	for _, control := range activeConfig.ControlAllowedIdentities {
		if decoded, err := hex.DecodeString(control); err == nil && len(decoded) == rns.ReticulumTruncatedHashLength/8 {
			messageRouter.AllowControl(decoded)
		}
	}

	if runPN || activeConfig.EnablePropagationNode {
		messageRouter.EnablePropagation()
		if messageRouter.PropagationDestination != nil {
			rns.Log("LXMF Propagation Node started on "+rns.PrettyHexRep(messageRouter.PropagationDestination.Hash()), rns.LOG_NOTICE)
		}
	}

	rns.Log("LXMF Router ready to receive on "+rns.PrettyHexRep(lxmfDestination.Hash()), rns.LOG_NOTICE)
	time.Sleep(100 * time.Millisecond)
	go deferredStartJobs()

	select {}
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func jobs() {
	for {
		if messageRouter != nil && lxmfDestination != nil && activeConfig.PeerAnnounceInterval > 0 {
			if time.Since(lastPeerAnnounce) >= activeConfig.PeerAnnounceInterval {
				messageRouter.Announce(lxmfDestination.Hash(), nil)
				lastPeerAnnounce = time.Now()
			}
		}
		if messageRouter != nil && activeConfig.NodeAnnounceInterval > 0 {
			if time.Since(lastNodeAnnounce) >= activeConfig.NodeAnnounceInterval {
				messageRouter.AnnouncePropagationNode()
				lastNodeAnnounce = time.Now()
			}
		}
		time.Sleep(jobsInterval)
	}
}

func deferredStartJobs() {
	time.Sleep(deferredJobsDelay)
	if messageRouter == nil || lxmfDestination == nil {
		return
	}
	rns.Log("Running deferred start jobs", rns.LOG_DEBUG)
	if activeConfig.PeerAnnounceAtStart {
		rns.Log("Sending announce for LXMF delivery destination", rns.LOG_EXTREME)
		messageRouter.Announce(lxmfDestination.Hash(), nil)
	}
	if activeConfig.NodeAnnounceAtStart {
		rns.Log("Sending announce for LXMF Propagation Node", rns.LOG_EXTREME)
		messageRouter.AnnouncePropagationNode()
	}
	lastPeerAnnounce = time.Now()
	lastNodeAnnounce = time.Now()
	go jobs()
}

func detectExistingConfigDir(configDir string) (string, error) {
	if configDir != "" {
		if fileExists(filepath.Join(configDir, "config")) {
			return configDir, nil
		}
		return "", fmt.Errorf("non-existent config path: %s", configDir)
	}
	if dirExists("/etc/lxmd") && fileExists("/etc/lxmd/config") {
		return "/etc/lxmd", nil
	}
	if userdir, err := os.UserHomeDir(); err == nil {
		candidate := filepath.Join(userdir, ".config/lxmd")
		if dirExists(candidate) && fileExists(filepath.Join(candidate, "config")) {
			return candidate, nil
		}
		fallback := filepath.Join(userdir, ".lxmd")
		if dirExists(fallback) && fileExists(filepath.Join(fallback, "config")) {
			return fallback, nil
		}
	}
	return "", errors.New("could not locate LXMD configuration directory")
}

func setRemotePaths(configDir string) (string, error) {
	resolved, err := detectExistingConfigDir(configDir)
	if err != nil {
		return "", err
	}
	configPath = filepath.Join(resolved, "config")
	ignoredPath = filepath.Join(resolved, "ignored")
	allowedPath = filepath.Join(resolved, "allowed")
	identityPath = filepath.Join(resolved, "identity")
	storageDir = filepath.Join(resolved, "storage")
	messagesDir = filepath.Join(storageDir, "messages")
	return resolved, nil
}

func remoteInit(configDir, rnsConfigDir, identityFile string, verbosity, quietness int) error {
	_, err := setRemotePaths(configDir)
	if err != nil {
		return err
	}
	if identityFile == "" {
		identityFile = filepath.Join(filepath.Dir(configPath), "identity")
	}
	if !fileExists(identityFile) {
		return fmt.Errorf("identity file not found: %s", identityFile)
	}

	lxmdConfig, err = configobj.Load(configPath)
	if err != nil {
		return fmt.Errorf("could not parse configuration: %w", err)
	}
	if err := applyConfig(); err != nil {
		return err
	}

	level := targetLogLevel + verbosity - quietness
	var logDest any = func(int, string) {}
	if _, err := rns.NewReticulum(ptrOrNil(rnsConfigDir), &level, logDest, nil, true, nil); err != nil {
		return fmt.Errorf("could not start Reticulum: %w", err)
	}

	identity, err = rns.IdentityFromFile(identityFile)
	if err != nil {
		return fmt.Errorf("could not load identity: %w", err)
	}
	return nil
}

func getRemoteIdentity(remote string, timeout float64) (*rns.Identity, error) {
	if remote == "" {
		if identity == nil {
			return nil, errors.New("local identity not initialised")
		}
		return identity, nil
	}
	destHash, err := hex.DecodeString(remote)
	if err != nil {
		return nil, fmt.Errorf("invalid remote destination hash: %w", err)
	}
	if len(destHash) != rns.ReticulumTruncatedHashLength/8 {
		return nil, fmt.Errorf("remote destination hash must be %d bytes", rns.ReticulumTruncatedHashLength/8)
	}

	if id := rns.IdentityRecall(destHash); id != nil {
		return id, nil
	}

	if !rns.TransportHasPath(destHash) {
		rns.TransportRequestPath(destHash)
	}
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for time.Now().Before(deadline) {
		if rns.TransportHasPath(destHash) {
			if id := rns.IdentityRecall(destHash); id != nil {
				return id, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, errors.New("could not recall remote identity")
}

func controlRequest(remote *rns.Identity, path string, data any, timeout float64) (any, error) {
	dest, err := rns.NewDestination(remote, rns.DestinationOUT, rns.DestinationSINGLE, lxmf.AppName, "propagation", "control")
	if err != nil {
		return nil, err
	}
	link, err := rns.NewOutgoingLink(dest, rns.LinkModeDefault, nil, nil)
	if err != nil {
		return nil, err
	}
	defer link.Teardown()
	linkDeadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for link.Status != rns.LinkActive && time.Now().Before(linkDeadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if link.Status != rns.LinkActive {
		return nil, errors.New("control link establishment timed out")
	}
	link.Identify(identity)

	receipt := link.Request(path, data, nil, nil, nil, timeout)
	if receipt == nil {
		return nil, errors.New("control request could not be sent")
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for !receipt.Concluded() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !receipt.Concluded() {
		return nil, errors.New("control request timed out")
	}
	if receipt.Status() == rns.ReceiptFailed {
		return nil, errors.New("control request failed")
	}
	return receipt.Response(), nil
}

func convertStatsValue(val any) any {
	switch mapped := val.(type) {
	case map[any]any:
		return convertStatsMap(mapped)
	case []any:
		out := make([]any, len(mapped))
		for i, entry := range mapped {
			out[i] = convertStatsValue(entry)
		}
		return out
	case []byte:
		return fmt.Sprintf("%x", mapped)
	default:
		return mapped
	}
}

func convertStatsMap(input map[any]any) map[string]any {
	out := make(map[string]any, len(input))
	for key, val := range input {
		out[fmt.Sprint(key)] = convertStatsValue(val)
	}
	return out
}

func printStatusResponse(remote string, showStatus, showPeers bool, timeout float64) error {
	targetIdentity, err := getRemoteIdentity(remote, timeout)
	if err != nil {
		return err
	}
	resp, err := controlRequest(targetIdentity, lxmf.StatsGetPath, nil, timeout)
	if err != nil {
		return err
	}
	rawMap, ok := resp.(map[any]any)
	if !ok {
		return fmt.Errorf("unexpected stats response: %T", resp)
	}
	stats := convertStatsMap(rawMap)

	fmt.Printf("\nLXMF Propagation Node running on %v, uptime is %v\n", stats["destination_hash"], rns.PrettyTime(float64(toInt(stats["uptime"])), false, false))
	if showStatus {
		if ms, ok := stats["messagestore"].(map[string]any); ok {
			bytes := toInt(ms["bytes"])
			limit := toInt(ms["limit"])
			count := toInt(ms["count"])
			util := ""
			if limit > 0 {
				util = fmt.Sprintf("%.2f%%", float64(bytes)/float64(limit)*100)
			}
			fmt.Printf("Messagestore contains %d messages, %s (%s utilised of %s)\n", count, rns.PrettySize(float64(bytes)), util, rns.PrettySize(float64(limit)))
		}
		fmt.Printf("Required propagation stamp cost is %v, flexibility is %v\n", stats["target_stamp_cost"], stats["stamp_cost_flexibility"])
		fmt.Printf("Peering cost is %v, max remote peering cost is %v\n", stats["peering_cost"], stats["max_peering_cost"])
		if fromStaticOnly, ok := stats["from_static_only"].(bool); ok && fromStaticOnly {
			fmt.Printf("Accepting propagated messages from static peers only\n")
		} else {
			fmt.Printf("Accepting propagated messages from all nodes\n")
		}
		fmt.Printf("%s message limit, %s sync limit\n", rns.PrettySize(float64(toInt(stats["propagation_limit"])*1000)), rns.PrettySize(float64(toInt(stats["sync_limit"])*1000)))

		peersMap, _ := stats["peers"].(map[string]any)
		totalPeers := toInt(stats["total_peers"])
		maxPeers := toInt(stats["max_peers"])
		discoveredPeers := toInt(stats["discovered_peers"])
		staticPeers := toInt(stats["static_peers"])

		availablePeers := 0
		unreachablePeers := 0
		peeredIncoming := 0
		peeredOutgoing := 0
		peeredRxBytes := 0
		peeredTxBytes := 0
		for _, entry := range peersMap {
			pm, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			if alive, ok := pm["alive"].(bool); ok && alive {
				availablePeers++
			} else {
				unreachablePeers++
			}
			msgs, _ := pm["messages"].(map[string]any)
			peeredIncoming += toInt(msgs["incoming"])
			peeredOutgoing += toInt(msgs["outgoing"])
			peeredRxBytes += toInt(pm["rx_bytes"])
			peeredTxBytes += toInt(pm["tx_bytes"])
		}

		fmt.Printf("\nPeers   : %d total (peer limit is %d)\n", totalPeers, maxPeers)
		fmt.Printf("          %d discovered, %d static\n", discoveredPeers, staticPeers)
		fmt.Printf("          %d available, %d unreachable\n", availablePeers, unreachablePeers)

		unpeeredIncoming := toInt(stats["unpeered_propagation_incoming"])
		unpeeredRxBytes := toInt(stats["unpeered_propagation_rx_bytes"])
		clients, _ := stats["clients"].(map[string]any)
		clientPropagationReceived := toInt(clients["client_propagation_messages_received"])
		clientPropagationServed := toInt(clients["client_propagation_messages_served"])

		totalIncoming := peeredIncoming + unpeeredIncoming + clientPropagationReceived
		totalRxBytes := peeredRxBytes + unpeeredRxBytes
		df := any(0)
		if totalIncoming != 0 {
			raw := float64(peeredOutgoing) / float64(totalIncoming)
			df = math.Round(raw*100) / 100
		}

		fmt.Printf("\nTraffic : %d messages received in total (%s)\n", totalIncoming, rns.PrettySize(float64(totalRxBytes)))
		fmt.Printf("          %d messages received from peered nodes (%s)\n", peeredIncoming, rns.PrettySize(float64(peeredRxBytes)))
		fmt.Printf("          %d messages received from unpeered nodes (%s)\n", unpeeredIncoming, rns.PrettySize(float64(unpeeredRxBytes)))
		fmt.Printf("          %d messages transferred to peered nodes (%s)\n", peeredOutgoing, rns.PrettySize(float64(peeredTxBytes)))
		fmt.Printf("          %d propagation messages received directly from clients\n", clientPropagationReceived)
		fmt.Printf("          %d propagation messages served to clients\n", clientPropagationServed)
		fmt.Printf("          Distribution factor is %v\n", df)
		fmt.Println("")
	}
	if showPeers {
		// Python prints a blank line before the peer list (even if no peers exist) when show_status is false.
		if !showStatus {
			fmt.Println("")
		}
		if peers, ok := stats["peers"].(map[string]any); ok && len(peers) > 0 {
			for _, entry := range peers {
				peerMap, ok := entry.(map[string]any)
				if !ok {
					continue
				}
				_ = peerMap
				// Detailed per-peer output parity is implemented later.
			}
		}
	}
	return nil
}

func toInt(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		i, _ := strconv.Atoi(v)
		return i
	default:
		return 0
	}
}

func requestSyncPeer(target, remote string, timeout float64) error {
	destHash, err := hex.DecodeString(target)
	if err != nil {
		return fmt.Errorf("invalid peer destination hash: %w", err)
	}
	if len(destHash) != rns.ReticulumTruncatedHashLength/8 {
		return fmt.Errorf("peer destination hash must be %d bytes", rns.ReticulumTruncatedHashLength/8)
	}
	remoteIdentity, err := getRemoteIdentity(remote, timeout)
	if err != nil {
		return err
	}
	if _, err := controlRequest(remoteIdentity, lxmf.SyncRequestPath, destHash, timeout); err != nil {
		return err
	}
	fmt.Printf("Sync requested for peer %s\n", target)
	return nil
}

func requestUnpeerPeer(target, remote string, timeout float64) error {
	destHash, err := hex.DecodeString(target)
	if err != nil {
		return fmt.Errorf("invalid peer destination hash: %w", err)
	}
	if len(destHash) != rns.ReticulumTruncatedHashLength/8 {
		return fmt.Errorf("peer destination hash must be %d bytes", rns.ReticulumTruncatedHashLength/8)
	}
	remoteIdentity, err := getRemoteIdentity(remote, timeout)
	if err != nil {
		return err
	}
	if _, err := controlRequest(remoteIdentity, lxmf.UnpeerRequestPath, destHash, timeout); err != nil {
		return err
	}
	fmt.Printf("Broke peering with %s\n", target)
	return nil
}

func handleRemoteCommands(configDir, rnsConfigDir, identityPath, remote string, status, peers bool, syncTarget, unpeerTarget string, timeout float64, verbosity, quietness int) error {
	if err := remoteInit(configDir, rnsConfigDir, identityPath, verbosity, quietness); err != nil {
		return err
	}
	if status || peers {
		if err := printStatusResponse(remote, status, peers, timeout); err != nil {
			return err
		}
	}
	if syncTarget != "" {
		if err := requestSyncPeer(syncTarget, remote, timeout); err != nil {
			return err
		}
	}
	if unpeerTarget != "" {
		if err := requestUnpeerPeer(unpeerTarget, remote, timeout); err != nil {
			return err
		}
	}
	return nil
}

func ptrOrNil(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func main() {
	configDir := flag.String("config", "", "path to alternative lxmd config directory")
	rnsConfigDir := flag.String("rnsconfig", "", "path to alternative Reticulum config directory")
	propagationNode := flag.Bool("propagation-node", false, "run an LXMF Propagation Node")
	onInbound := flag.String("on-inbound", "", "command run when a message is received")
	service := flag.Bool("service", false, "lxmd is running as a service and should log to file")
	statusFlag := flag.Bool("status", false, "display node status")
	peersFlag := flag.Bool("peers", false, "display peered nodes")
	syncTarget := flag.String("sync", "", "request a sync with the specified peer")
	unpeerTarget := flag.String("break", "", "break peering with the specified peer")
	timeout := flag.Float64("timeout", 5, "timeout for query operations")
	remote := flag.String("remote", "", "remote propagation node destination hash")
	identityPathOption := flag.String("identity", "", "path to identity used for remote requests")
	example := flag.Bool("exampleconfig", false, "print verbose configuration example and exit")
	version := flag.Bool("version", false, "print version and exit")

	var verboseCount int
	var quietCount int
	flag.Func("v", "increase verbosity", func(string) error { verboseCount++; return nil })
	flag.Func("verbose", "increase verbosity", func(string) error { verboseCount++; return nil })
	flag.Func("q", "increase quietness", func(string) error { quietCount++; return nil })
	flag.Func("quiet", "increase quietness", func(string) error { quietCount++; return nil })
	flag.Parse()

	if *example {
		fmt.Print(defaultConfigFile)
		return
	}
	if *version {
		fmt.Printf("lxmd %s\n", lxmf.Version)
		return
	}

	if *statusFlag || *peersFlag || *syncTarget != "" || *unpeerTarget != "" {
		if err := handleRemoteCommands(*configDir, *rnsConfigDir, *identityPathOption, *remote, *statusFlag, *peersFlag, *syncTarget, *unpeerTarget, *timeout, verboseCount, quietCount); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *timeout <= 0 {
		*timeout = 5
	}
	_ = *timeout
	_ = *remote
	_ = *identityPathOption

	programSetup(*configDir, *rnsConfigDir, *propagationNode, *onInbound, verboseCount, quietCount, *service)
}
