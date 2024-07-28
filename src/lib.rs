#![allow(unused_imports)]
#![allow(dead_code)]
tonic::include_proto!("mod");

use ruft::{
    AppendEntriesRequest, AppendEntriesResponse, Log, LogId, RequestVoteRequest,
    RequestVoteResponse,
};
use std::{collections::HashMap, error::Error};
use tokio::{
    sync::mpsc::{self, Receiver},
    time::{self, sleep, timeout, Duration},
};

mod candidate;
mod follower;
mod leader;
mod raft;

use raft::Raft;

pub trait Persistent {
    fn store() -> Result<(), Box<dyn Error>>;
}

pub trait Transport: Send + Sync + 'static {
    fn send<T>(&self, peer: NodeID, msg: T) -> Result<(), Box<dyn Error>>;
}

pub trait AsyncTransport: Send + Sync + 'static {
    async fn send<Q, P>(&self, peer: NodeID, msg: Q) -> Result<P, Box<dyn Error>>;
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum State {
    Leader,
    Candidate,
    Follower,
}

type Term = u32;

pub enum Message {
    AppendEntriesReq(AppendEntriesRequest),
    AppendEntriesRep(AppendEntriesResponse),
    AskForVoteReq(RequestVoteRequest),
    AskForVoteRep(RequestVoteResponse),
}

enum AskForVote {
    Req(RequestVoteRequest),
    Rep(RequestVoteResponse),
}

pub enum Request {
    AppendEntries(AppendEntriesRequest),
    RequestVote(RequestVoteRequest),
}

pub enum Response {
    AppendEntries(AppendEntriesResponse),
    RequestVote(RequestVoteResponse),
}

fn next_election_timeout() -> u64 {
    1000
}
type NodeID = u32;
type LogIndex = u64;
