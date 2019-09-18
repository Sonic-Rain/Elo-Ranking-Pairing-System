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
use rumqtt::{MqttClient, MqttOptions, QoS};

use std::thread;
use std::time::Duration;
use log::Level;
use serde_json::{self, Result, Value};
use regex::Regex;

use ::futures::Future;
use mysql;
use room::PrestartStatus;

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
                .help("MQTT server address (127.0.0.1)"),
        ).arg(
            Arg::with_name("PORT")
                .short("P")
                .long("port")
                .takes_value(true)
                .help("MQTT server port (1883)"),
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

    let server_addr = matches.value_of("SERVER").unwrap_or("127.0.0.1").to_owned();
    let server_port = matches.value_of("PORT").unwrap_or("1883").to_owned();
    let client_id = matches
        .value_of("CLIENT_ID")
        .map(|x| x.to_owned())
        .unwrap_or_else(generate_client_id);
    let mut mqtt_options = MqttOptions::new(client_id.as_str(), server_addr.as_str(), server_port.parse::<u16>().unwrap());
    mqtt_options = mqtt_options.set_keep_alive(100);
    let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options.clone()).unwrap();
    mqtt_client.subscribe("member/+/send/login", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("member/+/send/logout", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("member/+/send/choose_hero", QoS::AtLeastOnce).unwrap();

    mqtt_client.subscribe("room/+/send/create", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("room/+/send/close", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("room/+/send/start_queue", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("room/+/send/cancel_queue", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("room/+/send/invite", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("room/+/send/join", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("room/+/send/accept_join", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("room/+/send/kick", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("room/+/send/leave", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("room/+/send/prestart", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("room/+/send/start", QoS::AtLeastOnce).unwrap();

    mqtt_client.subscribe("game/+/send/game_over", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("game/+/send/start_game", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("game/+/send/choose", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("game/+/send/leave", QoS::AtLeastOnce).unwrap();
    mqtt_client.subscribe("game/+/send/exit", QoS::AtLeastOnce).unwrap();
    
    let (tx, rx):(Sender<MqttMsg>, Receiver<MqttMsg>) = bounded(1000);
    let pool = mysql::Pool::new(get_url().as_str()).unwrap();
    thread::sleep_ms(100);
    thread::spawn(move || {
        let mut mqtt_options = MqttOptions::new(generate_client_id(), server_addr, server_port.parse::<u16>().unwrap());
        mqtt_options = mqtt_options.set_keep_alive(100);
        let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options.clone()).unwrap();
        loop {
            select! {
                recv(rx) -> d => {
                    if let Ok(d) = d {
                        mqtt_client.publish(d.topic, QoS::AtLeastOnce, false, d.msg).unwrap();
                    }
                }
            }
        }
    });

    let relogin = Regex::new(r"\w+/(\w+)/send/login").unwrap();
    let relogout = Regex::new(r"\w+/(\w+)/send/logout").unwrap();
    let recreate = Regex::new(r"\w+/(\w+)/send/create").unwrap();
    let reclose = Regex::new(r"\w+/(\w+)/send/close").unwrap();
    let restart_queue = Regex::new(r"\w+/(\w+)/send/start_queue").unwrap();
    let recancel_queue = Regex::new(r"\w+/(\w+)/send/cancel_queue").unwrap();
    let represtart = Regex::new(r"\w+/(\w+)/send/prestart").unwrap();
    let reinvite = Regex::new(r"\w+/(\w+)/send/invite").unwrap();
    let rejoin = Regex::new(r"\w+/(\w+)/send/join").unwrap();
    let reset = Regex::new(r"reset").unwrap();
    let rechoosehero = Regex::new(r"\w+/(\w+)/send/choose_hero").unwrap();
    let releave = Regex::new(r"\w+/(\w+)/send/leave").unwrap();
    
    let mut sender: Sender<RoomEventData> = event_room::init(tx, pool.clone());
    
    loop {
        use rumqtt::Notification::Publish;
        select! {
            recv(notifications) -> notification => {
                if let Ok(x) = notification {
                    if let Publish(x) = x {
                        let payload = x.payload;
                        let msg = match str::from_utf8(&payload[..]) {
                            Ok(msg) => msg,
                            Err(err) => {
                                error!("Failed to decode publish message {:?}", err);
                                continue;
                            }
                        };
                        let topic_name = x.topic_name.as_str();
                        let vo : Result<Value> = serde_json::from_str(msg);
                        if reset.is_match(topic_name) {
                            info!("reset");
                            sender.send(RoomEventData::Reset());
                        }
                        let vo : Result<Value> = serde_json::from_str(msg);
                        if let Ok(v) = vo {
                            if reinvite.is_match(topic_name) {
                                let cap = reinvite.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("invite: userid: {} json: {:?}", userid, v);
                                event_room::invite(userid, v, sender.clone())?;
                            } else if rechoosehero.is_match(topic_name) {
                                let cap = rechoosehero.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("choose ng hero: userid: {} json: {:?}", userid, v);
                                event_room::choose_ng_hero(userid, v, sender.clone())?;
                            } else if rejoin.is_match(topic_name) {
                                let cap = rejoin.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("join: userid: {} json: {:?}", userid, v);
                                event_room::join(userid, v, sender.clone())?;
                            } else if relogin.is_match(topic_name) {
                                let cap = relogin.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("login: userid: {} json: {:?}", userid, v);
                                event_member::login(userid, v, pool.clone(), sender.clone())?;
                            } else if relogout.is_match(topic_name) {
                                let cap = relogout.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("logout: userid: {} json: {:?}", userid, v);
                                event_member::logout(userid, v, pool.clone(), sender.clone())?;
                            } else if recreate.is_match(topic_name) {
                                let cap = recreate.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("create: userid: {} json: {:?}", userid, v);
                                event_room::create(userid, v, sender.clone())?;
                            } else if reclose.is_match(topic_name) {
                                let cap = reclose.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("close: userid: {} json: {:?}", userid, v);
                                event_room::close(userid, v, sender.clone())?;
                            } else if restart_queue.is_match(topic_name) {
                                let cap = restart_queue.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("start_queue: userid: {} json: {:?}", userid, v);
                                event_room::start_queue(userid, v, sender.clone())?;
                            } else if recancel_queue.is_match(topic_name) {
                                let cap = recancel_queue.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("cancel_queue: userid: {} json: {:?}", userid, v);
                                event_room::cancel_queue(userid, v, sender.clone())?;
                            } else if represtart.is_match(topic_name) {
                                let cap = represtart.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("represtart: userid: {} json: {:?}", userid, v);
                                event_room::prestart(userid, v, sender.clone())?;
                            }
                        } else {
                            warn!("Json Parser error");
                        };
                    }
                }
            }
        }
    }
}
