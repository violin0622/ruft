use crate::leader::LeaderFn;
use crate::Transport;
use crate::{
    candidate, follower, leader, AppendEntriesRequest, AppendEntriesResponse, Log, LogId, LogIndex,
    Message, NodeID, Request, RequestVoteRequest, RequestVoteResponse, Response, State, Term,
};
use std::{collections::HashMap, error::Error};
use tokio::{
    sync::mpsc::{self, Receiver},
    time::{self, sleep, timeout, Duration},
};
use tonic::{IntoRequest, Response};

pub(crate) struct Raft<T: Transport + Send> {
    // persistent
    pub id: NodeID,
    pub peers: Vec<NodeID>,
    pub state: State,
    pub current_term: Term,
    // 已收到的日志
    // 环形缓存， 包含已持久化的日志。
    // 日志应用至状态机之后，即可将其清理并自增 committed_log_index
    // 日志提交之后，应用至状态机之前，需要持久化。 且存储于环形缓存中。
    // 提交需要自增 committed_log_index。
    // 总体应分为三段： 已应用已清理 -> 已应用未清理 -> 未应用已提交 -> 未应用未提交
    pub logs: Vec<Log>,

    // volatile
    pub committed_log_index: u64,
    // applied_log_index: u64,
    pub new_log_index: u64,

    // leader volatile
    // next log index to be send
    pub next_log_index: Option<HashMap<NodeID, LogIndex>>,
    // highest match log index
    // matched_log_index: Option<HashMap<NodeID, LogIndex>>,

    // client: ruft::raft_client::RaftClient<tonic::transport::Channel>,
    // timeout: std::time::SystemTime,
    pub in_rx: mpsc::Receiver<Message>,
    pub out_rx: mpsc::Receiver<Response>,
    // append_entries_tx: tokio::sync::mpsc::Sender<AppendEntriesRequest>,
    pub transport: T,
    pub election_timeout: u64,
}

impl<T: Transport + Send> Raft<T> {
    pub async fn new(transport: T) -> Self {
        let (_in_tx, in_rx) = tokio::sync::mpsc::channel(0);
        let (_out_tx, out_rx) = tokio::sync::mpsc::channel(0);
        Self {
            id: 0,
            peers: vec![],
            state: State::Follower,
            current_term: 0,
            logs: vec![],
            committed_log_index: 0,
            // applied_log_index: 0,
            new_log_index: 0,
            next_log_index: None,
            // matched_log_index: None,

            // client: ruft::raft_client::RaftClient::connect("http://[::1]:8000")
            //     .await
            //     .unwrap(),
            // timeout: std::time::SystemTime::now(),
            in_rx,
            out_rx,
            // append_entries_tx: tx,
            transport,
            election_timeout: 0,
        }
    }

    fn maybe_update_term(&mut self, term: u32) {
        if self.current_term < term {
            self.current_term = term;
            self.state = State::Follower;
        }
    }

    pub async fn handle_append_entries_as_follower(&mut self, req: AppendEntriesRequest) {
        self.maybe_return_to_follower(req.term);

        let mut rep = AppendEntriesResponse::default();
        rep.term = self.current_term;
        if self.current_term > req.term {
            _ = self
                .transport
                .send(req.leader_id, Response::AppendEntries(rep));
            return;
        }
        let log_ok = req.prev_log.map_or(true, |log| {
            let idx = log.index as usize;
            idx > 0
                && idx <= self.logs.len()
                && self.logs[idx].id.is_some_and(|id| id.term == log.term)
        });

        if !log_ok {
            _ = self
                .transport
                .send(req.leader_id, Response::AppendEntries(rep));
            return;
        }

        if req.entries.len() == 0 {
            rep.success = true;
            self.committed_log_index = req.leader_committed_index;
            _ = self
                .transport
                .send(req.leader_id, Response::AppendEntries(rep));
            return;
        }

        let Some(LogId {
            term: _prev_term,
            index: prev_idx,
        }) = req.prev_log
        else {
            _ = self
                .transport
                .send(req.leader_id, Response::AppendEntries(rep));
            return;
        };
        let prev_idx = prev_idx as usize;

        if self.logs.len() >= prev_idx + 1 {
            match (self.logs[prev_idx].id, req.entries[0].id) {
                // \/ /\ m.mentries /= <<>>
                //    /\ Len(log[i]) >= index
                //    /\ log[i][index].term = m.mentries[1].term
                (Some(term1), Some(term2)) if term1 == term2 => {
                    self.committed_log_index = req.leader_committed_index;
                    rep.success = true;
                    rep.term = self.current_term;
                    _ = self
                        .transport
                        .send(req.leader_id, Response::AppendEntries(rep));
                }

                // /\ m.mentries /= << >>
                // /\ Len(log[i]) >= index
                // /\ log[i][index].term /= m.mentries[1].term
                // /\ LET new == [index2 \in 1..(Len(log[i]) - 1) |->
                //                    log[i][index2]]
                //    IN log' = [log EXCEPT ![i] = new]
                // /\ UNCHANGED <<serverVars, commitIndex, messages>>
                (Some(_), Some(_)) => {
                    self.logs.truncate(prev_idx + 1);
                    rep.term = self.current_term;
                    _ = self
                        .transport
                        .send(req.leader_id, Response::AppendEntries(rep));
                }
                _ => unreachable!("both self and req should have log term"),
            }
        }

        self.logs.append(req.entries.clone().as_mut());
        rep.success = true;
        rep.term = self.current_term;
        _ = self
            .transport
            .send(req.leader_id, Response::AppendEntries(rep));
    }

    fn maybe_return_to_follower(&mut self, term: u32) {
        if self.state == State::Candidate && self.logs.last().unwrap().id.unwrap().term == term {
            self.state = State::Follower
        }
    }

    // if self state is follower, then turn to candidate
    // if req.logID >= self.logID, then reply ok
    pub async fn handle_request_vote(&mut self, _req: RequestVoteRequest) {}

    pub fn stop(self) {}

    pub fn start(mut self) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = sleep(Duration::from_millis(1000)) => self.state = State::Candidate,
                    Some(msg) = self.in_rx.recv() => {
                    // if let Some(msg) = self.in_rx.recv().await {
                        match self.state {
                            State::Leader => LeaderFn::new(&mut self).run().await,
                            State::Candidate => candidate::CandidateFn::new(&mut self).run().await,
                            State::Follower => match msg {
                                Message::AppendEntriesReq(req) => {
                                    self.maybe_update_term(req.term);
                                    self.handle_append_entries_as_follower(req);
                                }
                                Message::AppendEntriesRep(req) => {
                                    self.maybe_update_term(req.term);
                                }
                                Message::AskForVoteReq(req) => self.maybe_update_term(req.term),
                                Message::AskForVoteRep(req) => self.maybe_update_term(req.term),
                            },
                        }
                    }
                }
            }
        });
    }

    // 将 command 封装为 Log, 调用 append entries 将其发送至所有随从
    pub async fn proposal(&mut self, command: &[u8]) {
        let log = Log {
            id: Some(LogId {
                term: self.current_term,
                index: self.new_log_index,
            }),
            content: command.into(),
        };
        self.logs.push(log);
        self.new_log_index += 1;
        self.next_log_index
            .as_mut()
            .unwrap()
            .iter_mut()
            .for_each(|(_, v)| *v += 1);

        // self.next_log_index
        // .map(|x| x.iter_mut().map(|(k, v)| (k, v)));
    }
}
