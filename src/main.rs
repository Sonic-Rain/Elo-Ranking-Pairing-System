#![allow(warnings)]
use log::{info, warn, error, trace};

mod event_member;
mod event_room;
mod room;
mod msg;

use std::env;
use std::io::Write;
use std::io::Error;
use std::net::TcpStream;
use std::str;
use clap::{App, Arg};
use uuid::Uuid;

use mqtt::control::variable_header::ConnectReturnCode;
use mqtt::packet::*;
use mqtt::{Decodable, Encodable, QualityOfService};
use mqtt::{TopicFilter, TopicName};

use std::thread;
use std::time::Duration;
use log::Level;
use serde_json::{self, Result, Value};
use regex::Regex;

use ::futures::Future;
use mysql;

use crossbeam_channel::{bounded, tick, Sender, Receiver, select};
use crate::event_room::RoomEventData;
use crate::msg::*;

fn generate_client_id() -> String {
    format!("/MQTT/rust/{}", Uuid::new_v4())
}

fn get_url() -> String {
    "mysql://erps:erpsgogo@127.0.0.1:3306/erps".into()
}


fn main() -> std::result::Result<(), std::io::Error> {
    // configure logging
    env::set_var("RUST_LOG", env::var_os("RUST_LOG").unwrap_or_else(|| "info".into()));
    env_logger::init();

    let matches = App::new("erps")
        .author("damody <t1238142000@gmail.com>")
        .arg(
            Arg::with_name("SERVER")
                .short("S")
                .long("server")
                .takes_value(true)
                .help("MQTT server address (host:port)"),
        ).arg(
            Arg::with_name("USER_NAME")
                .short("u")
                .long("username")
                .takes_value(true)
                .help("Login user name"),
        ).arg(
            Arg::with_name("PASSWORD")
                .short("p")
                .long("password")
                .takes_value(true)
                .help("Password"),
        ).arg(
            Arg::with_name("CLIENT_ID")
                .short("i")
                .long("client-identifier")
                .takes_value(true)
                .help("Client identifier"),
        ).get_matches();

    let server_addr = matches.value_of("SERVER").unwrap_or("127.0.0.1:1883");
    let client_id = matches
        .value_of("CLIENT_ID")
        .map(|x| x.to_owned())
        .unwrap_or_else(generate_client_id);
    let mut channel_filters: Vec<(TopicFilter, QualityOfService)> = vec![
        (TopicFilter::new("member/+/send/login").unwrap(), QualityOfService::Level0),
        (TopicFilter::new("member/+/send/logout").unwrap(), QualityOfService::Level0),

        (TopicFilter::new("room/+/send/create").unwrap(), QualityOfService::Level0),
        (TopicFilter::new("room/+/send/close").unwrap(), QualityOfService::Level0),
        (TopicFilter::new("room/+/send/start_queue").unwrap(), QualityOfService::Level0),
        (TopicFilter::new("room/+/send/cancel_queue").unwrap(), QualityOfService::Level0),        
        (TopicFilter::new("room/+/send/invite").unwrap(), QualityOfService::Level0),
        (TopicFilter::new("room/+/send/join").unwrap(), QualityOfService::Level0),
        (TopicFilter::new("room/+/send/accept_join").unwrap(), QualityOfService::Level0),
        (TopicFilter::new("room/+/send/kick").unwrap(), QualityOfService::Level0),
        (TopicFilter::new("room/+/send/leave").unwrap(), QualityOfService::Level0),
        (TopicFilter::new("room/+/send/prestart").unwrap(), QualityOfService::Level0),
        (TopicFilter::new("room/+/send/start").unwrap(), QualityOfService::Level0),
    ];
    //= matches.values_of("SUBSCRIBE").unwrap().map(|c| (TopicFilter::new(c.to_string()).unwrap(), QualityOfService::Level0)).collect();

    //channel_filters.push();

    let keep_alive = 100;

    info!("Connecting to {:?} ... ", server_addr);
    let mut stream = TcpStream::connect(server_addr).unwrap();
    info!("Connected!");

    info!("Client identifier {:?}", client_id);
    let mut conn = ConnectPacket::new("MQTT", client_id);
    conn.set_clean_session(true);
    conn.set_keep_alive(keep_alive);
    let mut buf = Vec::new();
    conn.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();

    let connack = ConnackPacket::decode(&mut stream).unwrap();
    trace!("CONNACK {:?}", connack);

    if connack.connect_return_code() != ConnectReturnCode::ConnectionAccepted {
        panic!(
            "Failed to connect to server, return code {:?}",
            connack.connect_return_code()
        );
    }

    // const CHANNEL_FILTER: &'static str = "typing-speed-test.aoeu.eu";
    trace!("Applying channel filters {:?} ...", channel_filters);
    let sub = SubscribePacket::new(10, channel_filters);
    let mut buf = Vec::new();
    sub.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();

    loop {
        let packet = match VariablePacket::decode(&mut stream) {
            Ok(pk) => pk,
            Err(err) => {
                error!("Error in receiving packet {:?}", err);
                continue;
            }
        };
        trace!("PACKET {:?}", packet);

        if let VariablePacket::SubackPacket(ref ack) = packet {
            if ack.packet_identifier() != 10 {
                panic!("SUBACK packet identifier not match");
            }
            info!("Subscribed!");
            break;
        }
    }

    let mut stream_clone = stream.try_clone().unwrap();
    thread::spawn(move || {
        let mut last_ping_time = 0;
        let mut next_ping_time = last_ping_time + (keep_alive as f32 * 0.9) as i64;
        loop {
            let current_timestamp = time::get_time().sec;
            if keep_alive > 0 && current_timestamp >= next_ping_time {
                info!("Sending PINGREQ to broker");

                let pingreq_packet = PingreqPacket::new();

                let mut buf = Vec::new();
                pingreq_packet.encode(&mut buf).unwrap();
                stream_clone.write_all(&buf[..]).unwrap();

                last_ping_time = current_timestamp;
                next_ping_time = last_ping_time + (keep_alive as f32 * 0.9) as i64;
                thread::sleep(Duration::new((keep_alive / 2) as u64, 0));
            }
        }
    });
    let pool = mysql::Pool::new(get_url().as_str()).unwrap();

    let relogin = Regex::new(r"\w+/(\w+)/send/login").unwrap();
    let relogout = Regex::new(r"\w+/(\w+)/send/logout").unwrap();
    let recreate = Regex::new(r"\w+/(\w+)/send/create").unwrap();
    let reclose = Regex::new(r"\w+/(\w+)/send/close").unwrap();
    let restart_queue = Regex::new(r"\w+/(\w+)/send/start_queue").unwrap();
    let recancel_queue = Regex::new(r"\w+/(\w+)/send/cancel_queue").unwrap();
    let represtart = Regex::new(r"\w+/(\w+)/send/prestart").unwrap();
    
    let (tx, rx):(Sender<MqttMsg>, Receiver<MqttMsg>) = bounded(1000);
    let mut sender: Sender<RoomEventData> = event_room::init(tx);
    let mut stream2 = stream.try_clone().unwrap();
    thread::spawn(move || {
        loop {
            select! {
                recv(rx) -> d => {
                    if let Ok(d) = d {
                        let publish_packet = PublishPacket::new(TopicName::new(d.topic).unwrap(), QoSWithPacketIdentifier::Level0, d.msg.clone());
                        let mut buf = Vec::new();
                        publish_packet.encode(&mut buf).unwrap();
                        stream2.write_all(&buf[..]).unwrap();
                    }
                }
            }
        }
    });
    loop {
        let mut sender = sender.clone();
        let packet = match VariablePacket::decode(&mut stream) {
            Ok(pk) => pk,
            Err(err) => {
                error!("Error in receiving packet {}", err);
                continue;
            }
        };
        trace!("PACKET {:?}", packet);

        match packet {
            VariablePacket::PingrespPacket(..) => {
                info!("Receiving PINGRESP from broker ..");
            }
            VariablePacket::PublishPacket(ref publ) => {
                let msg = match str::from_utf8(&publ.payload_ref()[..]) {
                    Ok(msg) => msg,
                    Err(err) => {
                        error!("Failed to decode publish message {:?}", err);
                        continue;
                    }
                };
                
                let vo : Result<Value> = serde_json::from_str(msg);
                
                if let Ok(v) = vo {
                    if relogin.is_match(publ.topic_name()) {
                        let cap = relogin.captures(publ.topic_name()).unwrap();
                        let userid = cap[1].to_string();
                        info!("login: userid: {} json: {:?}", userid, v);
                        event_member::login(&mut stream, userid, v, pool.clone(), sender.clone())?;
                    } else if relogout.is_match(publ.topic_name()) {
                        let cap = relogout.captures(publ.topic_name()).unwrap();
                        let userid = cap[1].to_string();
                        info!("logout: userid: {} json: {:?}", userid, v);
                        event_member::logout(&mut stream, userid, v, pool.clone(), sender.clone())?;
                    } else if recreate.is_match(publ.topic_name()) {
                        let cap = recreate.captures(publ.topic_name()).unwrap();
                        let userid = cap[1].to_string();
                        info!("create: userid: {} json: {:?}", userid, v);
                        event_room::create(&mut stream, userid, v, pool.clone(), sender.clone())?;
                    } else if reclose.is_match(publ.topic_name()) {
                        let cap = reclose.captures(publ.topic_name()).unwrap();
                        let userid = cap[1].to_string();
                        info!("close: userid: {} json: {:?}", userid, v);
                        event_room::close(&mut stream, userid, v, pool.clone(), sender.clone())?;
                    } else if restart_queue.is_match(publ.topic_name()) {
                        let cap = restart_queue.captures(publ.topic_name()).unwrap();
                        let userid = cap[1].to_string();
                        info!("start_queue: userid: {} json: {:?}", userid, v);
                        event_room::start_queue(&mut stream, userid, v, pool.clone(), sender.clone())?;
                    } else if recancel_queue.is_match(publ.topic_name()) {
                        let cap = recancel_queue.captures(publ.topic_name()).unwrap();
                        let userid = cap[1].to_string();
                        info!("cancel_queue: userid: {} json: {:?}", userid, v);
                        event_room::cancel_queue(&mut stream, userid, v, pool.clone(), sender.clone())?;
                    } else if represtart.is_match(publ.topic_name()) {
                        let cap = represtart.captures(publ.topic_name()).unwrap();
                        let userid = cap[1].to_string();
                        info!("represtart: userid: {} json: {:?}", userid, v);
                        event_room::prestart(&mut stream, userid, v, pool.clone(), sender.clone())?;
                    }
                } else {
                    warn!("LoginData error");
                };
                
            }
            _ => {}
        }
    }
}
