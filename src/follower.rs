use crate::{
    AppendEntriesRequest, AppendEntriesResponse, LogId, Message, RequestVoteRequest,
    RequestVoteResponse,
};
use crate::{Raft, Request, Response, State, Transport};
use std::{collections::HashMap, error::Error};
use tokio::{
    sync::mpsc::{self, Receiver},
    time::{self, sleep, timeout, Duration},
};

pub(crate) struct FollowerFn<'a, T>
where
    T: Transport,
{
    raft: &'a mut Raft<T>,
}

impl<'a, T> FollowerFn<'a, T>
where
    T: Transport,
{
    pub fn new(raft: &'a mut Raft<T>) -> Self {
        Self { raft }
    }
    pub async fn run(mut self) {
        tokio::select! {
            _ = sleep(Duration::from_millis(1000)) => self.raft.state = State::Candidate,
            Some(req) = self.raft.in_rx.recv() => match req {
                Message::AppendEntriesReq(msg) => self.handle_append_entries_request(msg).await,
                Message::AppendEntriesRep(msg) => self.handle_append_entries_response(msg).await,
                Message::AskForVoteReq(msg) => self.handle_request_vote_request(msg).await,
                Message::AskForVoteRep(msg) => self.handle_request_vote_response(msg).await,
            }
        }
    }
    fn maybe_update_term(&mut self, term: u32) {
        if self.raft.current_term < term {
            self.raft.current_term = term;
            self.raft.state = State::Follower;
        }
    }

    /**
    ```tla+
    HandleAppendEntriesRequest(i,j,m) ==
        LET logOk == \/ m.mprevLogIndex = 0
                     \/ /\ m.prevLogIndex > 0
                        /\ m.prevLogIndex <= Len(log[i])
                        /\ m.prevLogTerm = log[i][m.mprevLogIndex].term
        IN /\ m.mterm <= currentTerm[i]
           /\ \/ /\ \/ m.mterm < currentTerm[i]
                    \/ /\ m.mterm = currentTerm[i]
                       /\ state[i] = Follower
                       /\ \not logOk
                 /\ Reply([
                      mtype       |-> AppendEntriesResponse,
                      mterm       |-> currentTerm[i],
                      msucc       |-> FALSE,
                      mmatchIndex |-> 0,
                      msource     |-> i,
                      mdest       |-> j,
                    ], m)
                 /\ UNCHANGED <<serverVars, logVars>>
              \/ \* return to follwer state.
                 /\ m.mterm = currentTerm[i]
                 /\ state[i] = Candidate
                 /\ state' = [state EXCEPT ![i] = Follower]
                 /\ UNCHANGED <<currrentTerm, votedFor, logVars, messages>>
              \/ \* accept request
                 /\ m.mterm = currentTerm[i]
                 /\ state[i] = Follower
                 /\ logOk
                 /\ LET index == m.mprevLogIndex + 1
                    IN \/ \* already done with request
                          /\ \/ m.mentries = <<>>
                             \/  /\ m.mentries /= <<>>
                                 /\ Len(log[i]) >= index
                                 /\ log[i][index].term = m.mentries[1].term
                             \* This could make our commitIndex decrease (for
                             \* example if we process an old, duplicated request),
                             \* but that doesn't really affect anything.
                          /\ commitIndex' = [commitIndex EXCEPT ![i] = m.mcommitIndex]
                          /\ Reply([mtype    |-> AppendEntriesResponse,
                             mterm           |-> currentTerm[i],
                             msuccess        |-> TRUE,
                             mmatchIndex     |-> m.mprevLogIndex + Len(m.mentries),
                             msource         |-> i,
                             mdest           |-> j], m)
                          /\ UNCHANGED <<serverVars, log>>
              \/ \* conflict: remove 1 entry
                 /\ m.mentries /= << >>
                 /\ Len(log[i]) >= index
                 /\ log[i][index].term /= m.mentries[1].term
                 /\ LET new == [index2 \in 1..(Len(log[i]) - 1) |->
                                    log[i][index2]]
                    IN log' = [log EXCEPT ![i] = new]
                 /\ UNCHANGED <<serverVars, commitIndex, messages>>
              \/ \* no conflict: append entry
                 /\ m.mentries /= << >>
                 /\ len(log[i]) = m.mprevlogindex
                 /\ log' = [log except ![i] =
                                append(log[i], m.mentries[1])]
                 /\ unchanged <<servervars, commitindex, messages>>
           /\ UNCHANGED <<candidateVars, leaderVars>>
    ```
    */
    async fn handle_append_entries_request(&mut self, mut req: AppendEntriesRequest) {
        self.maybe_update_term(req.term);
        let logOk = || {
            req.prev_log.map_or(true, |log| {
                log.index > 0
                    && log.index as usize <= self.raft.logs.len()
                    && log.term == self.raft.logs[log.index as usize].id.unwrap().term
            })
        };
        let mut rep = AppendEntriesResponse::default();
        if self.raft.current_term > req.term {
            rep.term = self.raft.current_term;
            rep.success = false;
            _ = self
                .raft
                .transport
                .send(req.leader_id, Response::AppendEntries(rep));
            return;
        }
        if req.term > self.raft.current_term {
            self.raft.current_term = req.term;
        }
        rep.term = self.raft.current_term;

        let matched_log = self
            .raft
            .logs
            .iter()
            .rfind(|&&log| match (log.id, req.prev_log) {
                (Some(id), Some(prev_log_id)) => id == prev_log_id,
                _ => false,
            });
        if None == matched_log {
            _ = self
                .raft
                .transport
                .send(req.leader_id, Response::AppendEntries(rep));
            return;
        }

        // 如果一个已经存在的条目和新条目冲突（索引相同， 任期不同），则将本地在该条目及其之后的所
        // 有日志条目删除
        // 本地储存的所有日志条目的任期，都小于等于最后一条日志的任期。
        // 如果最后一条日志的任期都小于等于请求中的任期，则所有日志的任期都小于等于请求中的任期。
        // 索引相同，任期不同；
        // 任期相同，索引不同；
        idx = self.raft.logs.len() - 1;
        let conflict;
        loop {
            let log_id = self.raft.logs[idx].id.clone().unwrap();
            if log_id.index == prev_log.index {
                conflict = true;
                break;
            }
            idx -= 1;
        }
        if conflict {
            self.raft.logs.drain(idx..);
        }

        // 追加所有新日志条目
        self.raft.logs.append(req.entries.as_mut());

        // 如果首领的已知已提交的最高日志条目的索引 大于 随从的已知已提交的最高日志条目的索引
        // (leaderCommit > commitIndex)，则把 commitIndex 重置为 leaderCommit
        if req.leader_committed_index > self.raft.committed_log_index {
            self.raft.committed_log_index = req.leader_committed_index;
        }

        rep.success = true;
        rep.term = self.raft.current_term;
        _ = self
            .raft
            .transport
            .send(req.leader_id, Response::AppendEntries(rep));
    }

    // 作为随从，不会发送该消息，如果收到该消息，有可能是之前作为首领发送的消息，但是由于网络原因
    // 未能及时收到，此时应该忽略该消息
    async fn handle_append_entries_response(&mut self, rep: AppendEntriesResponse) {}

    // 当收到 RequestVote 请求时，说明有某个成员发起了选举。
    // 1. 如果该成员的任期小于当前任期，说明该成员的选举已经过期，直接拒绝
    // 2. 如果该成员的任期大于当前任期，说明该成员的选举更加新，更新当前任期，然后拒绝
    // 3. 如果该成员的任期等于当前任期，说明该成员的选举和当前任期一样新，比较日志
    //   3.1 如果该成员的日志比当前成员的日志新，拒绝
    //   3.2 如果该成员的日志比当前成员的日志旧，同意
    //   3.3 如果该成员的日志和当前成员的日志一样新，比较 id
    //   3.3.1 如果该成员的 id 比当前成员的 id 大，拒绝
    //   3.3.2 如果该成员的 id 比当前成员的 id 小，同意
    //   3.3.3 如果该成员的 id 和当前成员的 id 一样大，拒绝
    //   3.3.4 如果该成员的 id 和当前成员的 id 一样小，同意
    //   3.3.5 如果该成员的 id 和当前成员的 id 一样，同意
    async fn handle_request_vote_request(&mut self, _req: RequestVoteRequest) {}
    async fn handle_request_vote_response(&mut self, _req: RequestVoteResponse) {}
}
