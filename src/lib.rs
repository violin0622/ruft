#![allow(unused_imports)]
#![allow(dead_code)]
tonic::include_proto!("mod");

use ruft::{
    AppendEntriesRequest, AppendEntriesResponse, LogId, RequestVoteRequest, RequestVoteResponse,
};
use std::{collections::HashMap, error::Error};
use tokio::{
    sync::mpsc::{self, Receiver},
    time::{self, sleep, timeout, Duration},
};

pub trait Persistent {
    fn store() -> Result<(), Box<dyn Error>>;
}

pub trait Transport: Send + 'static {
    fn send<T>(&self, peer: NodeID, msg: T) -> Result<(), Box<dyn Error>>;
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum State {
    Leader,
    Candidate,
    Follower,
}

type Term = u32;

struct LeaderFn<'a, T>
where
    T: Transport,
{
    raft: &'a mut Raft<T>,
}
impl<'a, T> LeaderFn<'a, T>
where
    T: Transport,
{
    pub fn new(raft: &'a mut Raft<T>) -> Self {
        Self { raft }
    }

    pub async fn run(self) {
        let mut interval = time::interval(time::Duration::from_millis(1000));
        // tokio::interval
        // handle requests
        tokio::select! {
            _ = interval.tick() => {
                for peer in &self.raft.peers {
                    _ = self.raft.transport.send(
                        *peer,
                        Message::AppendEntries(
                            AppendEntriesRequest{
                                term: self.raft.current_term,
                                leader_id: self.raft.id,
                                prev_log: Some(LogId {
                                    term: self.raft.current_term,
                                    index: self.raft.new_log_index-1,
                                }),
                                entries: vec![],
                                leader_committed_index: 0,
                            }
                        ),
                    );
                }
            }
            Some(req) = self.raft.rx.recv() => match req {
                Message::AppendEntries(_req) => {
                    // if req.term > self.raft.current_term
                },
                Message::RequestVote(_req) => {
                    // if req.term > self.raft.current_term
                },
            }
        }
    }
}

struct FollowerFn<'a, T>
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
            Some(req) = self.raft.rx.recv() => match req {
                // Message::AppendEntries(req) => self.raft.handle_append_entries(req).await,
                Message::AppendEntries(req) => self.handle_append_entries(req).await,
                Message::RequestVote(req) => self.raft.handle_request_vote(req).await,
            }
        }
    }

    async fn handle_append_entries(&mut self, mut req: AppendEntriesRequest) {
        let mut rep = AppendEntriesResponse::default();
        if self.raft.current_term > req.term {
            rep.term = self.raft.current_term;
            rep.success = false;
            _ = self
                .raft
                .transport
                .send(req.leader_id, Response::AppendEntries(rep));
            // return false
            return;
        } else if req.term > self.raft.current_term {
            self.raft.current_term = req.term;
        }
        rep.term = self.raft.current_term;

        let mut idx = self.raft.logs.len() - 1;
        let matched;
        let prev_log = req.prev_log.unwrap();
        loop {
            let log_id = self.raft.logs[idx].id.clone().unwrap();
            if log_id.term == prev_log.term && log_id.index == prev_log.index {
                matched = true;
                break;
            }
            idx -= 1;
        }
        if !matched {
            _ = self
                .raft
                .transport
                .send(req.leader_id, Response::AppendEntries(rep));
            return;
        }

        // 如果一个已经存在的条目和新条目冲突（索引相同， 任期不同），则将本地在该条目及其之后的所
        // 有日志条目删除
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
}

struct CandidateFn<'a, T>
where
    T: Transport,
{
    raft: &'a mut Raft<T>,
}

impl<'a, T> CandidateFn<'a, T>
where
    T: Transport,
{
    pub fn new(raft: &'a mut Raft<T>) -> Self {
        Self { raft }
    }

    // 向所有 Peers 送 request_vote 请求, 收到过半则转变为 Leader;
    // 收到新的 append_entries 则转变为 Follower;
    // 超时则开启新任期
    pub async fn run(self) {
        self.raft.current_term += 1;
        let mut counter = 1;
        let election_timeout = 1000;
        loop {
            tokio::select! {
                _ = sleep(Duration::from_millis(election_timeout)) => self.raft.state = State::Candidate,
                Some(req) = self.raft.rx.recv() => match req {
                    Message::AppendEntries(req) => self.raft.handle_append_entries(req).await,
                    Message::RequestVote(req) => self.raft.handle_request_vote(req).await,
                },
                Some(rep) = self.raft.rep_rx.recv() => match rep {
                    Response::AppendEntries(_rep) => { unreachable!() },
                    Response::RequestVote(rep) => {
                        if rep.granted {
                            counter += 1;
                            if counter > self.raft.peers.len() / 2 {
                                self.raft.state = State::Leader;
                                break;
                            }
                        }
                    },
                },
                else => {
                    for peer in &self.raft.peers{
                        _ = self.raft.transport.send(
                            *peer,
                            Message::RequestVote(
                                RequestVoteRequest{
                                    term: self.raft.current_term,
                                    candidate_id: self.raft.id,
                                    last_log_id: Some(LogId{
                                        term: self.raft.current_term-1,
                                        index: self.raft.new_log_index-1,
                                    }),
                                }
                            )
                        );
                    }
                }
            }
        }
    }
}

pub enum Message {
    AppendEntries(AppendEntriesRequest),
    RequestVote(RequestVoteRequest),
}

pub enum Response {
    AppendEntries(AppendEntriesResponse),
    RequestVote(RequestVoteResponse),
}

pub struct Raft<T: Transport + Send> {
    // persistent
    id: NodeID,
    peers: Vec<NodeID>,
    state: State,
    current_term: Term,
    // 已收到的日志
    // 环形缓存， 包含已持久化的日志。
    // 日志应用至状态机之后，即可将其清理并自增 committed_log_index
    // 日志提交之后，应用至状态机之前，需要持久化。 且存储于环形缓存中。
    // 提交需要自增 committed_log_index。
    // 总体应分为三段： 已应用已清理 -> 已应用未清理 -> 未应用已提交 -> 未应用未提交
    logs: Vec<ruft::Log>,

    // volatile
    committed_log_index: u64,
    // applied_log_index: u64,
    new_log_index: u64,

    // leader volatile
    // next log index to be send
    next_log_index: Option<HashMap<NodeID, LogIndex>>,
    // highest match log index
    // matched_log_index: Option<HashMap<NodeID, LogIndex>>,

    // client: ruft::raft_client::RaftClient<tonic::transport::Channel>,
    // timeout: std::time::SystemTime,
    rx: mpsc::Receiver<Message>,
    rep_rx: mpsc::Receiver<Response>,
    // append_entries_tx: tokio::sync::mpsc::Sender<AppendEntriesRequest>,
    transport: T,
    election_timeout: u64,
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

    async fn handle_append_entries(&mut self, _req: AppendEntriesRequest) {}

    // if self state is follower, then turn to candidate
    // if req.logID >= self.logID, then reply ok
    async fn handle_request_vote(&mut self, _req: RequestVoteRequest) {}

    pub fn stop(self) {}

    pub async fn start(mut self) {
        tokio::spawn(async move {
            loop {
                match &self.state {
                    State::Leader => LeaderFn::new(&mut self).run().await,
                    State::Candidate => CandidateFn::new(&mut self).run().await,
                    State::Follower => FollowerFn::new(&mut self).run().await,
                }
            }
        });
    }

    // 将 command 封装为 Log, 调用 append entries 将其发送至所有随从
    pub async fn proposal(&mut self, command: &[u8]) {
        let log = ruft::Log {
            id: Some(ruft::LogId {
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

fn next_election_timeout() -> u64 {
    1000
}
type NodeID = u32;
type LogIndex = u64;
