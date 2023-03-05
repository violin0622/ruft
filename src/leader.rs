use crate::{
    AppendEntriesRequest, AppendEntriesResponse, LogId, RequestVoteRequest, RequestVoteResponse,
};
use crate::{Message, Raft, Response, State, Transport};
use std::{collections::HashMap, error::Error};
use tokio::{
    sync::mpsc::{self, Receiver},
    time::{self, sleep, timeout, Duration},
};

pub(crate) struct LeaderFn<'a, T>
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
