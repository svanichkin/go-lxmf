package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
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

autopeer_maxdepth = 6

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
	PeerAnnounceInterval            *time.Duration
	DeliveryTransferMaxAcceptedSize float64
	OnInbound                       string

	EnablePropagationNode              bool
	NodeName                           string
	AuthRequired                       bool
	NodeAnnounceAtStart                bool
	AutoPeer                           bool
	AutoPeerMaxDepth                   *int
	NodeAnnounceInterval               *time.Duration
	MessageStorageLimitMB              float64
	PropagationTransferMaxAcceptedSize float64
	PropagationSyncMaxAcceptedSize     float64
	PropagationStampCostTarget         int
	PropagationStampCostFlexibility    int
	PeeringCost                        int
	RemotePeeringCostMax               int
	PrioritisedDestinations            []string
	ControlAllowedIdentities           []string
	StaticPeers                        [][]byte
	FromStaticOnly                     bool
	MaxPeers                           *int

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

func applyConfig() error {
	if lxmdConfig == nil {
		return errors.New("configuration missing")
	}

	activeConfig.DisplayName = "Anonymous Peer"
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("lxmf"); sec != nil {
			if value, ok := sec.Get("display_name"); ok {
				activeConfig.DisplayName = value
			}
		}
	}
	activeConfig.PeerAnnounceAtStart = false
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("lxmf"); sec != nil {
			if _, ok := sec.Get("announce_at_start"); ok {
				if value, err := sec.AsBool("announce_at_start"); err != nil {
					return fmt.Errorf("invalid lxmf.announce_at_start: %w", err)
				} else {
					activeConfig.PeerAnnounceAtStart = value
				}
			}
		}
	}
	if lxmdConfig != nil {
		sec := lxmdConfig.Section("lxmf")
		if sec != nil {
			if _, ok := sec.Get("announce_interval"); ok {
				value := time.Duration(0)
				v, err := sec.AsInt("announce_interval")
				if err != nil {
					return fmt.Errorf("invalid lxmf.announce_interval: %w", err)
				}
				value = time.Duration(v) * time.Minute
				activeConfig.PeerAnnounceInterval = &value
			}
		}
	}
	activeConfig.DeliveryTransferMaxAcceptedSize = 1000
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("lxmf"); sec != nil {
			if _, ok := sec.Get("delivery_transfer_max_accepted_size"); ok {
				if value, err := sec.AsFloat("delivery_transfer_max_accepted_size"); err != nil {
					return fmt.Errorf("invalid lxmf.delivery_transfer_max_accepted_size: %w", err)
				} else {
					activeConfig.DeliveryTransferMaxAcceptedSize = value
				}
			}
		}
	}
	if activeConfig.DeliveryTransferMaxAcceptedSize < 0.38 {
		activeConfig.DeliveryTransferMaxAcceptedSize = 0.38
	}
	activeConfig.OnInbound = ""
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("lxmf"); sec != nil {
			if value, ok := sec.Get("on_inbound"); ok {
				activeConfig.OnInbound = value
			}
		}
	}

	activeConfig.EnablePropagationNode = false
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if _, ok := sec.Get("enable_node"); ok {
				if value, err := sec.AsBool("enable_node"); err != nil {
					return fmt.Errorf("invalid propagation.enable_node: %w", err)
				} else {
					activeConfig.EnablePropagationNode = value
				}
			}
		}
	}
	activeConfig.NodeName = ""
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if value, ok := sec.Get("node_name"); ok {
				activeConfig.NodeName = value
			}
		}
	}
	activeConfig.AuthRequired = false
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if _, ok := sec.Get("auth_required"); ok {
				if value, err := sec.AsBool("auth_required"); err != nil {
					return fmt.Errorf("invalid propagation.auth_required: %w", err)
				} else {
					activeConfig.AuthRequired = value
				}
			}
		}
	}
	activeConfig.NodeAnnounceAtStart = false
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if _, ok := sec.Get("announce_at_start"); ok {
				if value, err := sec.AsBool("announce_at_start"); err != nil {
					return fmt.Errorf("invalid propagation.announce_at_start: %w", err)
				} else {
					activeConfig.NodeAnnounceAtStart = value
				}
			}
		}
	}
	activeConfig.AutoPeer = true
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if _, ok := sec.Get("autopeer"); ok {
				if value, err := sec.AsBool("autopeer"); err != nil {
					return fmt.Errorf("invalid propagation.autopeer: %w", err)
				} else {
					activeConfig.AutoPeer = value
				}
			}
		}
	}
	if lxmdConfig != nil {
		sec := lxmdConfig.Section("propagation")
		if sec != nil {
			if _, ok := sec.Get("autopeer_maxdepth"); ok {
				value := 0
				v, err := sec.AsInt("autopeer_maxdepth")
				if err != nil {
					return fmt.Errorf("invalid propagation.autopeer_maxdepth: %w", err)
				}
				value = v
				activeConfig.AutoPeerMaxDepth = &value
			}
			if _, ok := sec.Get("announce_interval"); ok {
				value := time.Duration(0)
				v, err := sec.AsInt("announce_interval")
				if err != nil {
					return fmt.Errorf("invalid propagation.announce_interval: %w", err)
				}
				value = time.Duration(v) * time.Minute
				activeConfig.NodeAnnounceInterval = &value
			}
		}
	}
	activeConfig.MessageStorageLimitMB = 500
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if _, ok := sec.Get("message_storage_limit"); ok {
				if value, err := sec.AsFloat("message_storage_limit"); err != nil {
					return fmt.Errorf("invalid propagation.message_storage_limit: %w", err)
				} else {
					activeConfig.MessageStorageLimitMB = value
				}
			}
		}
	}
	if activeConfig.MessageStorageLimitMB < 0.005 {
		activeConfig.MessageStorageLimitMB = 0.005
	}
	activeConfig.PropagationTransferMaxAcceptedSize = 256
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if _, ok := sec.Get("propagation_transfer_max_accepted_size"); ok {
				if value, err := sec.AsFloat("propagation_transfer_max_accepted_size"); err != nil {
					return fmt.Errorf("invalid propagation.propagation_transfer_max_accepted_size: %w", err)
				} else {
					activeConfig.PropagationTransferMaxAcceptedSize = value
				}
			}
		}
	}
	if lxmdConfig != nil {
		sec := lxmdConfig.Section("propagation")
		if sec != nil {
			if _, ok := sec.Get("propagation_message_max_accepted_size"); ok {
				if value, err := sec.AsFloat("propagation_message_max_accepted_size"); err != nil {
					return fmt.Errorf("invalid propagation.propagation_message_max_accepted_size: %w", err)
				} else {
					activeConfig.PropagationTransferMaxAcceptedSize = value
				}
			} else {
				activeConfig.PropagationTransferMaxAcceptedSize = 256
			}
		}
	}
	if activeConfig.PropagationTransferMaxAcceptedSize < 0.38 {
		activeConfig.PropagationTransferMaxAcceptedSize = 0.38
	}
	activeConfig.PropagationSyncMaxAcceptedSize = 256 * 40
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if _, ok := sec.Get("propagation_sync_max_accepted_size"); ok {
				if value, err := sec.AsFloat("propagation_sync_max_accepted_size"); err != nil {
					return fmt.Errorf("invalid propagation.propagation_sync_max_accepted_size: %w", err)
				} else {
					activeConfig.PropagationSyncMaxAcceptedSize = value
				}
			}
		}
	}
	if activeConfig.PropagationSyncMaxAcceptedSize < 0.38 {
		activeConfig.PropagationSyncMaxAcceptedSize = 0.38
	}
	activeConfig.PropagationStampCostTarget = 16
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if _, ok := sec.Get("propagation_stamp_cost_target"); ok {
				if value, err := sec.AsInt("propagation_stamp_cost_target"); err != nil {
					return fmt.Errorf("invalid propagation.propagation_stamp_cost_target: %w", err)
				} else {
					activeConfig.PropagationStampCostTarget = value
				}
			}
		}
	}
	activeConfig.PropagationStampCostFlexibility = 3
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if _, ok := sec.Get("propagation_stamp_cost_flexibility"); ok {
				if value, err := sec.AsInt("propagation_stamp_cost_flexibility"); err != nil {
					return fmt.Errorf("invalid propagation.propagation_stamp_cost_flexibility: %w", err)
				} else {
					activeConfig.PropagationStampCostFlexibility = value
				}
			}
		}
	}
	activeConfig.PeeringCost = 18
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if _, ok := sec.Get("peering_cost"); ok {
				if value, err := sec.AsInt("peering_cost"); err != nil {
					return fmt.Errorf("invalid propagation.peering_cost: %w", err)
				} else {
					activeConfig.PeeringCost = value
				}
			}
		}
	}
	activeConfig.RemotePeeringCostMax = 26
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if _, ok := sec.Get("remote_peering_cost_max"); ok {
				if value, err := sec.AsInt("remote_peering_cost_max"); err != nil {
					return fmt.Errorf("invalid propagation.remote_peering_cost_max: %w", err)
				} else {
					activeConfig.RemotePeeringCostMax = value
				}
			}
		}
	}
	activeConfig.PrioritisedDestinations = []string{}
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if value, ok := sec.Get("prioritise_destinations"); ok {
				activeConfig.PrioritisedDestinations = []string{}
				for _, part := range strings.Split(value, ",") {
					if trimmed := strings.TrimSpace(part); trimmed != "" {
						activeConfig.PrioritisedDestinations = append(activeConfig.PrioritisedDestinations, trimmed)
					}
				}
			}
		}
	}
	activeConfig.ControlAllowedIdentities = []string{}
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if value, ok := sec.Get("control_allowed"); ok {
				activeConfig.ControlAllowedIdentities = []string{}
				for _, part := range strings.Split(value, ",") {
					if trimmed := strings.TrimSpace(part); trimmed != "" {
						activeConfig.ControlAllowedIdentities = append(activeConfig.ControlAllowedIdentities, trimmed)
					}
				}
			}
		}
	}
	activeConfig.StaticPeers = [][]byte{}
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if value, ok := sec.Get("static_peers"); ok {
				activeConfig.StaticPeers = [][]byte{}
				for _, part := range strings.Split(value, ",") {
					if peer := strings.TrimSpace(part); peer != "" {
						decoded, err := hex.DecodeString(peer)
						if err != nil {
							rns.Log("Could not decode hash from: "+peer, rns.LOG_DEBUG)
							rns.Log("The contained exception was: "+err.Error(), rns.LOG_DEBUG)
							continue
						}
						activeConfig.StaticPeers = append(activeConfig.StaticPeers, decoded)
					}
				}
			}
		}
	}
	activeConfig.FromStaticOnly = false
	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("propagation"); sec != nil {
			if _, ok := sec.Get("from_static_only"); ok {
				if value, err := sec.AsBool("from_static_only"); err != nil {
					return fmt.Errorf("invalid propagation.from_static_only: %w", err)
				} else {
					activeConfig.FromStaticOnly = value
				}
			}
		}
	}
	if lxmdConfig != nil {
		sec := lxmdConfig.Section("propagation")
		if sec != nil {
			if _, ok := sec.Get("max_peers"); ok {
				value := 0
				v, err := sec.AsInt("max_peers")
				if err != nil {
					return fmt.Errorf("invalid propagation.max_peers: %w", err)
				}
				value = v
				activeConfig.MaxPeers = &value
			}
		}
	}

	activeConfig.IgnoredLXMFDestinations = [][]byte{}
	if file, err := os.Open(ignoredPath); err == nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if decoded, err := hex.DecodeString(line); err == nil {
				activeConfig.IgnoredLXMFDestinations = append(activeConfig.IgnoredLXMFDestinations, decoded)
			} else {
				rns.Log("Could not decode hash from: "+line, rns.LOG_DEBUG)
				rns.Log("The contained exception was: "+err.Error(), rns.LOG_DEBUG)
			}
		}
		_ = file.Close()
	} else if _, err := os.Stat(ignoredPath); err == nil {
		rns.Log("Error while loading list of ignored destinations: "+err.Error(), rns.LOG_ERROR)
	}
	activeConfig.AllowedIdentities = [][]byte{}
	if file, err := os.Open(allowedPath); err == nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if decoded, err := hex.DecodeString(line); err == nil {
				activeConfig.AllowedIdentities = append(activeConfig.AllowedIdentities, decoded)
			} else {
				rns.Log("Could not decode hash from: "+line, rns.LOG_DEBUG)
				rns.Log("The contained exception was: "+err.Error(), rns.LOG_DEBUG)
			}
		}
		_ = file.Close()
	} else if _, err := os.Stat(allowedPath); err == nil {
		rns.Log("Error while loading list of allowed identities: "+err.Error(), rns.LOG_ERROR)
	}

	if lxmdConfig != nil {
		if sec := lxmdConfig.Section("logging"); sec != nil {
			if _, ok := sec.Get("loglevel"); ok {
				if value, err := sec.AsInt("loglevel"); err != nil {
					return fmt.Errorf("invalid logging.loglevel: %w", err)
				} else {
					targetLogLevel = value
				}
			}
		}
	}

	return nil
}

func lxmfDelivery(msg *lxmf.LXMessage) {
	msgLabel := "<nil>"
	if msg != nil {
		msgLabel = msg.String()
	}
	defer func() {
		if rec := recover(); rec != nil {
			rns.Log("Error occurred while processing received message "+msgLabel+". The contained exception was: "+fmt.Sprint(rec), rns.LOG_ERROR)
		}
	}()

	written, err := msg.WriteToDirectory(messagesDir)
	if err != nil {
		panic(err)
	}
	rns.Log("Received "+msgLabel+" written to "+written, rns.LOG_DEBUG)
	if activeConfig.OnInbound != "" {
		rns.Log("Calling external program to handle message", rns.LOG_DEBUG)
		processingCommand := activeConfig.OnInbound + " \"" + written + "\""
		parts := make([]string, 0, 4)
		var current strings.Builder
		inSingleQuotes := false
		inDoubleQuotes := false
		escaping := false
		for _, r := range processingCommand {
			switch {
			case escaping:
				current.WriteRune(r)
				escaping = false
			case inSingleQuotes:
				if r == '\'' {
					inSingleQuotes = false
				} else {
					current.WriteRune(r)
				}
			case inDoubleQuotes:
				if r == '"' {
					inDoubleQuotes = false
				} else if r == '\\' {
					escaping = true
				} else {
					current.WriteRune(r)
				}
			case r == '\\':
				escaping = true
			case r == '"':
				inDoubleQuotes = true
			case r == '\'':
				inSingleQuotes = true
			case (r == ' ' || r == '\t' || r == '\n') && !inSingleQuotes && !inDoubleQuotes:
				if current.Len() > 0 {
					parts = append(parts, current.String())
					current.Reset()
				}
			default:
				current.WriteRune(r)
			}
		}
		if escaping || inSingleQuotes || inDoubleQuotes {
			panic(errors.New("unmatched quote in inbound command"))
		}
		if current.Len() > 0 {
			parts = append(parts, current.String())
		}
		if len(parts) == 0 {
			panic(errors.New("empty inbound command"))
		}
		cmd := exec.Command(parts[0], parts[1:]...)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		if err := cmd.Start(); err != nil {
			panic(err)
		}
		_ = cmd.Wait()
	} else {
		rns.Log("No action defined for inbound messages, ignoring", rns.LOG_DEBUG)
	}
}

func programSetup(configDir, rnsConfigDir string, runPN bool, onInbound string, verbosity, quietness int, service bool) {
	if onInbound != "" {
		activeConfig.OnInbound = onInbound
	}

	if configDir == "" {
		if info, err := os.Stat("/etc/lxmd"); err == nil && info.IsDir() {
			if _, err := os.Stat("/etc/lxmd/config"); err == nil {
				configDir = "/etc/lxmd"
			}
		} else if userdir, err := os.UserHomeDir(); err == nil {
			candidate := filepath.Join(userdir, ".config/lxmd")
			if info, err := os.Stat(candidate); err == nil && info.IsDir() {
				if _, err := os.Stat(filepath.Join(candidate, "config")); err == nil {
					configDir = candidate
				} else {
					configDir = filepath.Join(userdir, ".lxmd")
				}
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

	if _, err := os.Stat(configPath); err != nil {
		rns.Log("Could not load config file, creating default configuration file...", rns.LOG_WARNING)
		if err := os.WriteFile(configPath, []byte(defaultConfigFile), 0o644); err != nil {
			rns.Log("Failed to create default config: "+err.Error(), rns.LOG_ERROR)
			os.Exit(1)
		}
		rns.Log("Default config file created. Make any necessary changes in "+configPath+" and restart lxmd if needed.", rns.LOG_INFO)
		time.Sleep(1500 * time.Millisecond)
	}

	var err error
	lxmdConfig, err = configobj.Load(configPath)
	if err != nil {
		rns.Log("Could not parse the configuration at "+configPath, rns.LOG_ERROR)
		rns.Log("Check your configuration file for errors!", rns.LOG_ERROR)
		rns.Panic()
	}

	if err := applyConfig(); err != nil {
		rns.Log("Could not apply LXM Daemon configuration. The contained exception was: "+err.Error(), rns.LOG_ERROR)
		rns.Panic()
	}
	rns.Log("Configuration loaded from "+configPath, rns.LOG_VERBOSE)

	targetLogLevel = targetLogLevel + verbosity - quietness

	var logDest any = rns.LOG_STDOUT
	if service {
		logDest = rns.LOG_FILE
	}

	rns.Log("Substantiating Reticulum...", rns.LOG_NOTICE)
	var rnsConfigDirPtr *string
	if rnsConfigDir != "" {
		rnsConfigDirPtr = &rnsConfigDir
	}
	if _, err := rns.NewReticulum(rnsConfigDirPtr, &targetLogLevel, logDest, nil, false, nil); err != nil {
		rns.Log("Could not start Reticulum: "+err.Error(), rns.LOG_ERROR)
		os.Exit(1)
	}

	if _, err := os.Stat(identityPath); err == nil {
		identity, err = rns.IdentityFromFile(identityPath)
		if err != nil {
			rns.Log("Could not load identity: "+err.Error(), rns.LOG_ERROR)
			os.Exit(1)
		}
	} else {
		rns.Log("No Primary Identity file found, creating new...", rns.LOG_INFO)
		identity, err = rns.NewIdentity()
		if err != nil {
			rns.Log("Could not create identity: "+err.Error(), rns.LOG_ERROR)
			os.Exit(1)
		}
		if err := identity.Save(identityPath); err != nil {
			rns.Log("Could not save identity: "+err.Error(), rns.LOG_ERROR)
			os.Exit(1)
		}
		rns.Log("Created new Primary Identity "+identity.String(), rns.LOG_INFO)
	}

	messageRouter, err = lxmf.NewLXMRouter(identity, storageDir)
	if err != nil {
		rns.Log("Could not start LXMF router: "+err.Error(), rns.LOG_ERROR)
		os.Exit(1)
	}

	messageRouter.AutoPeer = activeConfig.AutoPeer
	if activeConfig.AutoPeerMaxDepth != nil {
		messageRouter.AutoPeerMaxDepth = *activeConfig.AutoPeerMaxDepth
	}
	if activeConfig.PeerAnnounceInterval != nil {
		lastPeerAnnounce = time.Now().Add(-*activeConfig.PeerAnnounceInterval)
	}
	if activeConfig.NodeAnnounceInterval != nil {
		lastNodeAnnounce = time.Now().Add(-*activeConfig.NodeAnnounceInterval)
	}
	messageRouter.PropagationPerTransferLimit = int(activeConfig.PropagationTransferMaxAcceptedSize)
	messageRouter.PropagationPerSyncLimit = int(activeConfig.PropagationSyncMaxAcceptedSize)
	messageRouter.DeliveryPerTransferLimit = int(activeConfig.DeliveryTransferMaxAcceptedSize)
	messageRouter.PropagationStampCost = activeConfig.PropagationStampCostTarget
	messageRouter.PropagationStampCostFlexibility = activeConfig.PropagationStampCostFlexibility
	messageRouter.PeeringCost = activeConfig.PeeringCost
	messageRouter.MaxPeeringCost = activeConfig.RemotePeeringCostMax
	if activeConfig.MaxPeers != nil {
		messageRouter.MaxPeers = *activeConfig.MaxPeers
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

	var displayNamePtr *string
	if activeConfig.DisplayName != "" {
		displayNamePtr = &activeConfig.DisplayName
	}
	lxmfDestination = messageRouter.RegisterDeliveryIdentity(identity, displayNamePtr, nil)
	rns.IdentityRemember(nil, lxmfDestination.Hash, identity.GetPublicKey(), nil)
	if activeConfig.AuthRequired {
		messageRouter.SetAuthentication(true)
		if len(activeConfig.AllowedIdentities) == 0 {
			rns.Log("Clint authentication was enabled, but no identity hashes could be loaded from "+allowedPath+". Nobody will be able to sync messages from this propagation node.", rns.LOG_WARNING)
		}
		for _, allowed := range activeConfig.AllowedIdentities {
			if len(allowed) == rns.ReticulumTruncatedHashLength/8 {
				messageRouter.Allow(allowed)
			}
		}
	}

	rns.Log("LXMF Router ready to receive on "+rns.PrettyHexRep(lxmfDestination.Hash), rns.LOG_NOTICE)
	if runPN || activeConfig.EnablePropagationNode {
		_ = messageRouter.SetMessageStorageLimit(0, activeConfig.MessageStorageLimitMB, 0)
		for _, dest := range activeConfig.PrioritisedDestinations {
			if decoded, err := hex.DecodeString(dest); err == nil && len(decoded) == rns.ReticulumTruncatedHashLength/8 {
				messageRouter.Prioritise(decoded)
			} else if err != nil {
				rns.Log("Cannot prioritise "+dest+", it is not a valid destination hash", rns.LOG_ERROR)
			}
		}
		for _, control := range activeConfig.ControlAllowedIdentities {
			if decoded, err := hex.DecodeString(control); err == nil && len(decoded) == rns.ReticulumTruncatedHashLength/8 {
				messageRouter.AllowControl(decoded)
			} else if err != nil {
				rns.Log("Cannot allow control from "+control+", it is not a valid identity hash", rns.LOG_ERROR)
			}
		}
		messageRouter.EnablePropagation()
		if messageRouter.PropagationDestination != nil {
			rns.Log("LXMF Propagation Node started on "+rns.PrettyHexRep(messageRouter.PropagationDestination.Hash), rns.LOG_NOTICE)
		}
	}

	rns.Log(fmt.Sprintf("Started lxmd version %s", lxmf.Version), rns.LOG_NOTICE)
	time.Sleep(100 * time.Millisecond)
	go deferredStartJobs()

	select {}
}

func jobs() {
	for {
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					rns.Log("An error occurred while running periodic jobs. The contained exception was: "+fmt.Sprint(rec), rns.LOG_ERROR)
				}
			}()
			if activeConfig.PeerAnnounceInterval != nil {
				if time.Since(lastPeerAnnounce) >= *activeConfig.PeerAnnounceInterval {
					messageRouter.Announce(lxmfDestination.Hash, nil)
					lastPeerAnnounce = time.Now()
				}
			}
			if activeConfig.NodeAnnounceInterval != nil {
				if time.Since(lastNodeAnnounce) >= *activeConfig.NodeAnnounceInterval {
					messageRouter.AnnouncePropagationNode()
					lastNodeAnnounce = time.Now()
				}
			}
		}()
		time.Sleep(jobsInterval)
	}
}

func deferredStartJobs() {
	time.Sleep(deferredJobsDelay)
	rns.Log("Running deferred start jobs", rns.LOG_DEBUG)
	if activeConfig.PeerAnnounceAtStart {
		rns.Log("Sending announce for LXMF delivery destination", rns.LOG_EXTREME)
		messageRouter.Announce(lxmfDestination.Hash, nil)
	}
	if activeConfig.NodeAnnounceAtStart {
		rns.Log("Sending announce for LXMF Propagation Node", rns.LOG_EXTREME)
		messageRouter.AnnouncePropagationNode()
	}
	lastPeerAnnounce = time.Now()
	lastNodeAnnounce = time.Now()
	go jobs()
}

func remoteInit(configDir, rnsConfigDir, identityFile string, verbosity, quietness int) error {
	var err error
	if identityFile == "" {
		resolved := configDir
		if resolved == "" {
			if info, err := os.Stat("/etc/lxmd"); err == nil && info.IsDir() {
				if _, err := os.Stat("/etc/lxmd/config"); err == nil {
					resolved = "/etc/lxmd"
				}
			}
			if resolved == "" {
				if userdir, err := os.UserHomeDir(); err == nil {
					candidate := filepath.Join(userdir, ".config/lxmd")
					if info, err := os.Stat(candidate); err == nil && info.IsDir() {
						if _, err := os.Stat(filepath.Join(candidate, "config")); err == nil {
							resolved = candidate
						}
					}
					if resolved == "" {
						fallback := filepath.Join(userdir, ".lxmd")
						if info, err := os.Stat(fallback); err == nil && info.IsDir() {
							if _, err := os.Stat(filepath.Join(fallback, "config")); err == nil {
								resolved = fallback
							}
						}
					}
				}
			}
		}
		if resolved == "" {
			return errors.New("could not locate LXMD configuration directory")
		}
		if _, err := os.Stat(filepath.Join(resolved, "config")); err != nil {
			return fmt.Errorf("non-existent config path: %s", resolved)
		}
		configPath = filepath.Join(resolved, "config")
		identityPath = filepath.Join(resolved, "identity")
	}
	if identityFile == "" {
		identityFile = filepath.Join(filepath.Dir(configPath), "identity")
	}
	if _, err := os.Stat(identityFile); err != nil {
		return fmt.Errorf("identity file not found: %s", identityFile)
	}

	level := targetLogLevel + verbosity - quietness
	var logDest any = func(int, string) {}
	var rnsConfigDirPtr *string
	if rnsConfigDir != "" {
		rnsConfigDirPtr = &rnsConfigDir
	}
	if _, err := rns.NewReticulum(rnsConfigDirPtr, &level, logDest, nil, true, nil); err != nil {
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

	if !rns.HasPath(destHash) {
		rns.RequestPath(destHash, nil, nil, false)
	}
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for time.Now().Before(deadline) {
		if rns.HasPath(destHash) {
			return rns.IdentityRecall(destHash), nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, errors.New("could not recall remote identity")
}

func renderStatusResponse(w io.Writer, stats map[string]any, showStatus, showPeers bool) {
	intValue := func(value any) int {
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
	floatValue := func(value any) float64 {
		switch v := value.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int:
			return float64(v)
		case int64:
			return float64(v)
		case uint64:
			return float64(v)
		case string:
			f, _ := strconv.ParseFloat(v, 64)
			return f
		default:
			return 0
		}
	}

	fmt.Fprintf(w, "\nLXMF Propagation Node running on %v, uptime is %v\n", stats["destination_hash"], rns.PrettyTime(float64(intValue(stats["uptime"])), false, false))
	if showStatus {
		ms := stats["messagestore"].(map[string]any)
		bytes := intValue(ms["bytes"])
		limit := intValue(ms["limit"])
		count := intValue(ms["count"])
		util := ""
		if limit > 0 {
			util = fmt.Sprintf("%.2f%%", float64(bytes)/float64(limit)*100)
		}
		fmt.Fprintf(w, "Messagestore contains %d messages, %s (%s utilised of %s)\n", count, rns.PrettySize(float64(bytes)), util, rns.PrettySize(float64(limit)))
		fmt.Fprintf(w, "Required propagation stamp cost is %v, flexibility is %v\n", stats["target_stamp_cost"], stats["stamp_cost_flexibility"])
		fmt.Fprintf(w, "Peering cost is %v, max remote peering cost is %v\n", stats["peering_cost"], stats["max_peering_cost"])
		if stats["from_static_only"].(bool) {
			fmt.Fprintln(w, "Accepting propagated messages from static peers only")
		} else {
			fmt.Fprintln(w, "Accepting propagated messages from all nodes")
		}
		fmt.Fprintf(w, "%s message limit, %s sync limit\n", rns.PrettySize(float64(intValue(stats["propagation_limit"])*1000)), rns.PrettySize(float64(intValue(stats["sync_limit"])*1000)))

		peersMap := stats["peers"].(map[string]any)
		totalPeers := intValue(stats["total_peers"])
		maxPeers := intValue(stats["max_peers"])
		discoveredPeers := intValue(stats["discovered_peers"])
		staticPeers := intValue(stats["static_peers"])

		availablePeers := 0
		unreachablePeers := 0
		peeredIncoming := 0
		peeredOutgoing := 0
		peeredRxBytes := 0
		peeredTxBytes := 0
		for _, entry := range peersMap {
			pm := entry.(map[string]any)
			if pm["alive"].(bool) {
				availablePeers++
			} else {
				unreachablePeers++
			}
			msgs := pm["messages"].(map[string]any)
			peeredIncoming += intValue(msgs["incoming"])
			peeredOutgoing += intValue(msgs["outgoing"])
			peeredRxBytes += intValue(pm["rx_bytes"])
			peeredTxBytes += intValue(pm["tx_bytes"])
		}

		fmt.Fprintf(w, "\nPeers   : %d total (peer limit is %d)\n", totalPeers, maxPeers)
		fmt.Fprintf(w, "          %d discovered, %d static\n", discoveredPeers, staticPeers)
		fmt.Fprintf(w, "          %d available, %d unreachable\n", availablePeers, unreachablePeers)

		unpeeredIncoming := intValue(stats["unpeered_propagation_incoming"])
		unpeeredRxBytes := intValue(stats["unpeered_propagation_rx_bytes"])
		clients := stats["clients"].(map[string]any)
		clientPropagationReceived := intValue(clients["client_propagation_messages_received"])
		clientPropagationServed := intValue(clients["client_propagation_messages_served"])

		totalIncoming := peeredIncoming + unpeeredIncoming + clientPropagationReceived
		totalRxBytes := peeredRxBytes + unpeeredRxBytes
		df := 0.0
		if totalIncoming != 0 {
			raw := float64(peeredOutgoing) / float64(totalIncoming)
			df = math.RoundToEven(raw*100) / 100
		}

		fmt.Fprintf(w, "\nTraffic : %d messages received in total (%s)\n", totalIncoming, rns.PrettySize(float64(totalRxBytes)))
		fmt.Fprintf(w, "          %d messages received from peered nodes (%s)\n", peeredIncoming, rns.PrettySize(float64(peeredRxBytes)))
		fmt.Fprintf(w, "          %d messages received from unpeered nodes (%s)\n", unpeeredIncoming, rns.PrettySize(float64(unpeeredRxBytes)))
		fmt.Fprintf(w, "          %d messages transferred to peered nodes (%s)\n", peeredOutgoing, rns.PrettySize(float64(peeredTxBytes)))
		fmt.Fprintf(w, "          %d propagation messages received directly from clients\n", clientPropagationReceived)
		fmt.Fprintf(w, "          %d propagation messages served to clients\n", clientPropagationServed)
		fmt.Fprintf(w, "          Distribution factor is %v\n", df)
		fmt.Fprintln(w, "")
	}

	if showPeers {
		if !showStatus {
			fmt.Fprintln(w, "")
		}
		if len(stats["peers"].(map[string]any)) > 0 {
			for peerID, entry := range stats["peers"].(map[string]any) {
				peerMap := entry.(map[string]any)
				ind := "  "
				peerType := "Unknown peer    "
				switch peerMap["type"] {
				case "static":
					peerType = "Static peer     "
				case "discovered":
					peerType = "Discovered peer "
				}
				status := "Unreachable"
				if peerMap["alive"].(bool) {
					status = "Available"
				}
				h := math.Max(float64(time.Now().UnixNano())/1e9-float64(intValue(peerMap["last_heard"])), 0)
				hops := intValue(peerMap["network_distance"])
				hs := "hops unknown"
				if hops != rns.PathfinderMaxHops {
					if hops == 1 {
						hs = "1 hop away"
					} else {
						hs = fmt.Sprintf("%d hops away", hops)
					}
				}
				pm := peerMap["messages"].(map[string]any)
				pk := "Not generated"
				if peerMap["peering_key"] != nil {
					pk = fmt.Sprintf("Generated, value is %v", peerMap["peering_key"])
				}
				pc := peerMap["peering_cost"]
				psc := peerMap["target_stamp_cost"]
				psf := peerMap["stamp_cost_flexibility"]
				if pc == nil {
					pc = "unknown"
				}
				if psc == nil {
					psc = "unknown"
				}
				if psf == nil {
					psf = "unknown"
				}
				ls := "never synced"
				if intValue(peerMap["last_sync_attempt"]) != 0 {
					lsa := math.Max(float64(time.Now().UnixNano())/1e9-float64(intValue(peerMap["last_sync_attempt"])), 0)
					ls = fmt.Sprintf("last synced %s ago", rns.PrettyTime(lsa, false, false))
				}
				sstr := rns.PrettySpeed(float64(intValue(peerMap["str"])))
				sler := rns.PrettySpeed(float64(intValue(peerMap["ler"])))
				stl := "Unknown"
				if intValue(peerMap["transfer_limit"]) != 0 {
					stl = rns.PrettySize(float64(intValue(peerMap["transfer_limit"]) * 1000))
				}
				ssl := "unknown"
				if intValue(peerMap["sync_limit"]) != 0 {
					ssl = rns.PrettySize(float64(intValue(peerMap["sync_limit"]) * 1000))
				}
				srxb := rns.PrettySize(float64(intValue(peerMap["rx_bytes"])))
				stxb := rns.PrettySize(float64(intValue(peerMap["tx_bytes"])))
				pmo := intValue(pm["offered"])
				pmout := intValue(pm["outgoing"])
				pmi := intValue(pm["incoming"])
				pmuh := intValue(pm["unhandled"])
				ar := math.RoundToEven(floatValue(peerMap["acceptance_rate"])*10000) / 100
				nn := strings.TrimSpace(fmt.Sprint(peerMap["name"]))
				if nn == "<nil>" {
					nn = ""
				}
				nn = strings.NewReplacer("\n", "", "\r", "").Replace(nn)
				if len(nn) > 45 {
					nn = nn[:45] + "..."
				}

				fmt.Fprintf(w, "%s%s%s\n", ind, peerType, rns.PrettyHexRep([]byte(peerID)))
				if nn != "" {
					fmt.Fprintf(w, "%sName       : %s\n", ind+ind, nn)
				}
				fmt.Fprintf(w, "%sStatus     : %s, %s, last heard %s ago\n", ind+ind, status, hs, rns.PrettyTime(h, false, false))
				fmt.Fprintf(w, "%sCosts      : Propagation %v (flex %v), peering %v\n", ind+ind, psc, psf, pc)
				fmt.Fprintf(w, "%sSync key   : %s\n", ind+ind, pk)
				fmt.Fprintf(w, "%sSpeeds     : %s STR, %s LER\n", ind+ind, sstr, sler)
				fmt.Fprintf(w, "%sLimits     : %s message limit, %s sync limit\n", ind+ind, stl, ssl)
				fmt.Fprintf(w, "%sMessages   : %d offered, %d outgoing, %d incoming, %.2f%% acceptance rate\n", ind+ind, pmo, pmout, pmi, ar)
				fmt.Fprintf(w, "%sTraffic    : %s received, %s sent\n", ind+ind, srxb, stxb)
				ms := "s"
				if pmuh == 1 {
					ms = ""
				}
				fmt.Fprintf(w, "%sSync state : %d unhandled message%s, %s\n", ind+ind, pmuh, ms, ls)
			}
		}
	}
}

func printStatusResponse(remote string, showStatus, showPeers bool, timeout float64) error {
	targetIdentity, err := getRemoteIdentity(remote, timeout)
	if err != nil {
		return err
	}
	dest, err := rns.NewDestination(targetIdentity, rns.DestinationOUT, rns.DestinationSINGLE, lxmf.AppName, "propagation", "control")
	if err != nil {
		return err
	}
	link, err := rns.NewLink(dest, nil, rns.LinkModeDefault, nil, nil)
	if err != nil {
		return err
	}
	defer link.Teardown()
	linkDeadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for link.Status != rns.LinkActive && time.Now().Before(linkDeadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if link.Status != rns.LinkActive {
		return errors.New("control link establishment timed out")
	}
	link.Identify(identity)
	time.Sleep(50 * time.Millisecond)
	request := link.Request(lxmf.StatsGetPath, nil, nil, nil, nil, timeout)
	if request == nil {
		return errors.New("control request could not be sent")
	}
	receipt, ok := request.(*rns.RequestReceipt)
	if !ok || receipt == nil {
		return fmt.Errorf("unexpected control request receipt: %T", request)
	}
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for {
		status := receipt.GetStatus()
		if status == rns.RequestReceiptReady || status == rns.RequestReceiptFailed {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("control request timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if receipt.GetStatus() == rns.RequestReceiptFailed {
		return errors.New("control request failed")
	}
	resp := receipt.GetResponse()
	rawMap, ok := resp.(map[any]any)
	if !ok {
		return fmt.Errorf("unexpected stats response: %T", resp)
	}
	var normalize func(any) any
	normalize = func(val any) any {
		switch mapped := val.(type) {
		case map[any]any:
			out := make(map[string]any, len(mapped))
			for key, entry := range mapped {
				switch k := key.(type) {
				case []byte:
					out[string(k)] = normalize(entry)
				default:
					out[fmt.Sprint(k)] = normalize(entry)
				}
			}
			return out
		case []any:
			out := make([]any, len(mapped))
			for i, entry := range mapped {
				out[i] = normalize(entry)
			}
			return out
		case []byte:
			return fmt.Sprintf("%x", mapped)
		default:
			return mapped
		}
	}
	stats := normalize(rawMap).(map[string]any)
	renderStatusResponse(os.Stdout, stats, showStatus, showPeers)
	return nil
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
	dest, err := rns.NewDestination(remoteIdentity, rns.DestinationOUT, rns.DestinationSINGLE, lxmf.AppName, "propagation", "control")
	if err != nil {
		return err
	}
	link, err := rns.NewLink(dest, nil, rns.LinkModeDefault, nil, nil)
	if err != nil {
		return err
	}
	defer link.Teardown()
	linkDeadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for link.Status != rns.LinkActive && time.Now().Before(linkDeadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if link.Status != rns.LinkActive {
		return errors.New("control link establishment timed out")
	}
	link.Identify(identity)
	time.Sleep(50 * time.Millisecond)
	request := link.Request(lxmf.SyncRequestPath, destHash, nil, nil, nil, timeout)
	if request == nil {
		return errors.New("control request could not be sent")
	}
	receipt, ok := request.(*rns.RequestReceipt)
	if !ok || receipt == nil {
		return fmt.Errorf("unexpected control request receipt: %T", request)
	}
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for {
		status := receipt.GetStatus()
		if status == rns.RequestReceiptReady || status == rns.RequestReceiptFailed {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("control request timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if receipt.GetStatus() == rns.RequestReceiptFailed {
		return errors.New("control request failed")
	}
	response := receipt.GetResponse()
	if response == nil {
		return errors.New("empty response received")
	}
	switch code := response.(type) {
	case int:
		switch code {
		case lxmf.PeerErrorNoIdentity:
			return errors.New("control request rejected: remote side has not identified this link")
		case lxmf.PeerErrorNoAccess:
			return errors.New("control request rejected: access denied")
		case lxmf.PeerErrorInvalidData:
			return errors.New("control request rejected: invalid request data")
		case lxmf.PeerErrorNotFound:
			return errors.New("control request rejected: peer not found")
		}
	case int64:
		switch int(code) {
		case lxmf.PeerErrorNoIdentity:
			return errors.New("control request rejected: remote side has not identified this link")
		case lxmf.PeerErrorNoAccess:
			return errors.New("control request rejected: access denied")
		case lxmf.PeerErrorInvalidData:
			return errors.New("control request rejected: invalid request data")
		case lxmf.PeerErrorNotFound:
			return errors.New("control request rejected: peer not found")
		}
	case float64:
		switch int(code) {
		case lxmf.PeerErrorNoIdentity:
			return errors.New("control request rejected: remote side has not identified this link")
		case lxmf.PeerErrorNoAccess:
			return errors.New("control request rejected: access denied")
		case lxmf.PeerErrorInvalidData:
			return errors.New("control request rejected: invalid request data")
		case lxmf.PeerErrorNotFound:
			return errors.New("control request rejected: peer not found")
		}
	case nil:
	}
	fmt.Printf("Sync requested for peer %s\n", rns.PrettyHexRep(destHash))
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
	dest, err := rns.NewDestination(remoteIdentity, rns.DestinationOUT, rns.DestinationSINGLE, lxmf.AppName, "propagation", "control")
	if err != nil {
		return err
	}
	link, err := rns.NewLink(dest, nil, rns.LinkModeDefault, nil, nil)
	if err != nil {
		return err
	}
	defer link.Teardown()
	linkDeadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for link.Status != rns.LinkActive && time.Now().Before(linkDeadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if link.Status != rns.LinkActive {
		return errors.New("control link establishment timed out")
	}
	link.Identify(identity)
	time.Sleep(50 * time.Millisecond)
	request := link.Request(lxmf.UnpeerRequestPath, destHash, nil, nil, nil, timeout)
	if request == nil {
		return errors.New("control request could not be sent")
	}
	receipt, ok := request.(*rns.RequestReceipt)
	if !ok || receipt == nil {
		return fmt.Errorf("unexpected control request receipt: %T", request)
	}
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for {
		status := receipt.GetStatus()
		if status == rns.RequestReceiptReady || status == rns.RequestReceiptFailed {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("control request timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if receipt.GetStatus() == rns.RequestReceiptFailed {
		return errors.New("control request failed")
	}
	response := receipt.GetResponse()
	if response == nil {
		return errors.New("empty response received")
	}
	switch code := response.(type) {
	case int:
		switch code {
		case lxmf.PeerErrorNoIdentity:
			return errors.New("control request rejected: remote side has not identified this link")
		case lxmf.PeerErrorNoAccess:
			return errors.New("control request rejected: access denied")
		case lxmf.PeerErrorInvalidData:
			return errors.New("control request rejected: invalid request data")
		case lxmf.PeerErrorNotFound:
			return errors.New("control request rejected: peer not found")
		}
	case int64:
		switch int(code) {
		case lxmf.PeerErrorNoIdentity:
			return errors.New("control request rejected: remote side has not identified this link")
		case lxmf.PeerErrorNoAccess:
			return errors.New("control request rejected: access denied")
		case lxmf.PeerErrorInvalidData:
			return errors.New("control request rejected: invalid request data")
		case lxmf.PeerErrorNotFound:
			return errors.New("control request rejected: peer not found")
		}
	case float64:
		switch int(code) {
		case lxmf.PeerErrorNoIdentity:
			return errors.New("control request rejected: remote side has not identified this link")
		case lxmf.PeerErrorNoAccess:
			return errors.New("control request rejected: access denied")
		case lxmf.PeerErrorInvalidData:
			return errors.New("control request rejected: invalid request data")
		case lxmf.PeerErrorNotFound:
			return errors.New("control request rejected: peer not found")
		}
	case nil:
	}
	fmt.Printf("Broke peering with %s\n", rns.PrettyHexRep(destHash))
	return nil
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
	timeout := flag.Float64("timeout", 0, "timeout for query operations")
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

	if *statusFlag || *peersFlag {
		if *timeout <= 0 {
			*timeout = 5
		}
		if err := remoteInit(*configDir, *rnsConfigDir, *identityPathOption, verboseCount, quietCount); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := printStatusResponse(*remote, *statusFlag, *peersFlag, *timeout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *syncTarget != "" {
		if *timeout <= 0 {
			*timeout = 10
		}
		if err := remoteInit(*configDir, *rnsConfigDir, *identityPathOption, verboseCount, quietCount); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := requestSyncPeer(*syncTarget, *remote, *timeout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *unpeerTarget != "" {
		if *timeout <= 0 {
			*timeout = 10
		}
		if err := remoteInit(*configDir, *rnsConfigDir, *identityPathOption, verboseCount, quietCount); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := requestUnpeerPeer(*unpeerTarget, *remote, *timeout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	programSetup(*configDir, *rnsConfigDir, *propagationNode, *onInbound, verboseCount, quietCount, *service)
}
