const std = @import("std");

fn randTimeout() u64 {
    return std.crypto.random.intRangeAtMost(u64, 400, 750);
}

const Role = enum { Follower, Candidate, Leader };

const LogEntry = struct { term: u32, command: []const u8 };

pub const Node = struct {
    id: u32,
    role: Role,
    currentTerm: u32,
    votedFor: ?u32,
    log: std.ArrayList(LogEntry),
    commitIndex: u32,
    lastApplied: u32,
    electionTimeout: u64,
    lastHeartbeatTime: i64,
    allocator: std.mem.Allocator,

    pub fn new(id: u32) !Node {
        const allocator = std.heap.page_allocator;
        const list = try std.ArrayList(LogEntry).initCapacity(allocator, 32);
        return .{ .id = id, .role = .Follower, .currentTerm = 0, .votedFor = null, .log = list, .commitIndex = 0, .lastApplied = 0, .electionTimeout = randTimeout(), .lastHeartbeatTime = std.time.milliTimestamp(), .allocator = allocator };
    }

    pub fn resetElectionTimeout(self: *Node, now: i64) void {
        self.electionTimeout = randTimeout();
        self.lastHeartbeatTime = now;
    }

    pub fn checkElectionTimeout(self: *const Node, now: i64) bool {
        return now - self.lastHeartbeatTime >= self.electionTimeout;
    }

    pub fn appendLog(self: *Node, term: u32, command: []const u8) !void {
        const entry = LogEntry{
            .term = term,
            .command = command,
        };
        try self.log.append(self.allocator, entry);
    }

    pub fn applyCommitted(self: *Node) void {
        while (self.lastApplied < self.commitIndex) : (self.lastApplied += 1) {
            const entry = self.log.items[self.lastApplied];
            std.debug.print("Applying log[{}]: {s}\n", .{ self.lastApplied, entry.command });
        }
    }

    pub fn heartbeat(self: *Node, leaderId: u32, leaderTerm: u32, now: i64) void {
        if (leaderTerm >= self.currentTerm) {
            self.role = .Follower;
            self.currentTerm = leaderTerm;
            self.votedFor = null;
        }
        self.lastHeartbeatTime = now;
        std.debug.print("Follower {d} got heartbeat from Leader {d}\n", .{ self.id, leaderId });
    }

    pub fn requestVote(self: *Node, candidateTerm: u32, candidateId: u32, candidateLastLogIndex: u32, candidateLastLogTerm: u32, now: i64) bool {
        if (candidateTerm < self.currentTerm) return false;
        if (candidateTerm > self.currentTerm) {
            self.currentTerm = candidateTerm;
            self.votedFor = null;
            self.role = .Follower;
        }
        if (self.votedFor != null and self.votedFor.? != candidateId) {
            return false;
        }
        const myLastLogIndex = if (self.log.items.len == 0) 0 else self.log.items.len - 1;
        const myLastLogTerm = if (self.log.items.len == 0) 0 else self.log.items[myLastLogIndex].term;
        const candidateUpToDate = candidateLastLogTerm > myLastLogTerm or (candidateLastLogTerm == myLastLogTerm and candidateLastLogIndex >= myLastLogIndex);
        if (candidateUpToDate and (self.votedFor == null or self.votedFor.? == candidateId)) {
            self.votedFor = candidateId;
            self.lastHeartbeatTime = now;
            return true;
        }
        return false;
    }

    pub fn becomeLeader(self: *Node) void {
        std.debug.assert(self.role == .Candidate);
        self.role = .Leader;
        std.debug.print("Node {d} transitioning to leader\n", .{self.id});
    }
};
