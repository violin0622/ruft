use crate::Transport;
use crate::{
    candidate, follower, leader, AppendEntriesRequest, AppendEntriesResponse, Log, LogId, LogIndex,
    Message, NodeID, RequestVoteRequest, RequestVoteResponse, Response, State, Term,
};
use std::{collections::HashMap, error::Error};
use tokio::{
    sync::mpsc::{self, Receiver},
    time::{self, sleep, timeout, Duration},
};

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
    pub rx: mpsc::Receiver<Message>,
    pub rep_rx: mpsc::Receiver<Response>,
    // append_entries_tx: tokio::sync::mpsc::Sender<AppendEntriesRequest>,
    pub transport: T,
    pub election_timeout: u64,
}

impl<T: Transport + Send> Raft<T> {
    pub async fn new(transport: T) -> Self {
        let (_tx, rx) = tokio::sync::mpsc::channel(0);
        let (_tx, rep_rx) = tokio::sync::mpsc::channel(0);
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
            rx,
            rep_rx,
            // append_entries_tx: tx,
            transport,
            election_timeout: 0,
        }
    }

    pub async fn handle_append_entries(&mut self, _req: AppendEntriesRequest) {}

    // if self state is follower, then turn to candidate
    // if req.logID >= self.logID, then reply ok
    pub async fn handle_request_vote(&mut self, _req: RequestVoteRequest) {}

    pub fn stop(self) {}

    pub async fn start(mut self) {
        tokio::spawn(async move {
            loop {
                match &self.state {
                    State::Leader => leader::LeaderFn::new(&mut self).run().await,
                    State::Candidate => candidate::CandidateFn::new(&mut self).run().await,
                    State::Follower => follower::FollowerFn::new(&mut self).run().await,
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
