#![allow(warnings)]
use log::{info, warn, error, trace};

mod event_member;
mod event_room;
mod room;
mod msg;
mod elo;

use std::env;
use std::io::Write;
use failure::Error;
use std::net::TcpStream;
use std::str;
use clap::{App, Arg};
use uuid::Uuid;
use rumqtt::{MqttClient, MqttOptions, QoS};

use std::panic;
use std::thread;
use std::time::Duration;
use log::Level;
use serde_json::{self, Value};
use regex::Regex;

use ::futures::Future;
use mysql;
use room::PrestartStatus;

use crossbeam_channel::{bounded, tick, Sender, Receiver, select};
use crate::event_room::RoomEventData;
use crate::event_room::SqlData;
use crate::msg::*;

fn generate_client_id() -> String {
    format!("/MQTT/rust/{}", Uuid::new_v4())
}

fn get_url() -> String {
    "mysql://erps:erpsgogo@127.0.0.1:3306/erps".into()
}

fn main() -> std::result::Result<(), Error> {
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
    let mut mqtt_options = MqttOptions::new(client_id.as_str(), server_addr.as_str(), server_port.parse::<u16>()?);
    mqtt_options = mqtt_options.set_keep_alive(100);
    mqtt_options = mqtt_options.set_request_channel_capacity(10000);
    mqtt_options = mqtt_options.set_notification_channel_capacity(500000);
    let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options.clone())?;
    mqtt_client.subscribe("member/+/send/login", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("member/+/send/logout", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("member/+/send/choose_hero", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("member/+/send/status", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("member/+/send/reconnect", QoS::AtLeastOnce)?;

    mqtt_client.subscribe("room/+/send/create", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("room/+/send/close", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("room/+/send/start_queue", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("room/+/send/cancel_queue", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("room/+/send/invite", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("room/+/send/join", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("room/+/send/accept_join", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("room/+/send/kick", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("room/+/send/leave", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("room/+/send/prestart", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("room/+/send/prestart_get", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("room/+/send/start", QoS::AtLeastOnce)?;

    mqtt_client.subscribe("game/+/send/game_close", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("game/+/send/game_over", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("game/+/send/game_info", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("game/+/send/start_game", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("game/+/send/choose", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("game/+/send/leave", QoS::AtLeastOnce)?;
    mqtt_client.subscribe("game/+/send/exit", QoS::AtLeastOnce)?;
    
    let (tx, rx):(Sender<MqttMsg>, Receiver<MqttMsg>) = bounded(10000);
    let pool = mysql::Pool::new(get_url().as_str())?;
    thread::sleep_ms(100);
    thread::spawn(move || -> Result<(), Error> {
        
        let mut mqtt_options = MqttOptions::new(generate_client_id(), server_addr, server_port.parse::<u16>()?);
        mqtt_options = mqtt_options.set_keep_alive(100);
        mqtt_options = mqtt_options.set_request_channel_capacity(10000);
        mqtt_options = mqtt_options.set_notification_channel_capacity(10000);
        println!("mqtt_options {:#?}", mqtt_options);
        let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options.clone())?;
        loop {
            select! {
                recv(rx) -> d => {
                    if let Ok(d) = d {
                        let msg_res = mqtt_client.publish(d.topic, QoS::AtLeastOnce, false, d.msg);
                        match msg_res {
                            Ok(_) =>{},
                            Err(x) => {
                                panic!("{}", x);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    });

    let relogin = Regex::new(r"\w+/(\w+)/send/login")?;
    let relogout = Regex::new(r"\w+/(\w+)/send/logout")?;
    let recreate = Regex::new(r"\w+/(\w+)/send/create")?;
    let reclose = Regex::new(r"\w+/(\w+)/send/close")?;
    let restart_queue = Regex::new(r"\w+/(\w+)/send/start_queue")?;
    let recancel_queue = Regex::new(r"\w+/(\w+)/send/cancel_queue")?;
    let represtart_get = Regex::new(r"\w+/(\w+)/send/prestart_get")?;
    let represtart = Regex::new(r"\w+/(\w+)/send/prestart")?;
    let reinvite = Regex::new(r"\w+/(\w+)/send/invite")?;
    let rejoin = Regex::new(r"\w+/(\w+)/send/join")?;
    let reset = Regex::new(r"reset")?;
    let rechoosehero = Regex::new(r"\w+/(\w+)/send/choose_hero")?;
    let releave = Regex::new(r"\w+/(\w+)/send/leave")?;
    let restart_game = Regex::new(r"\w+/(\w+)/send/start_game")?;
    let regame_over = Regex::new(r"\w+/(\w+)/send/game_over")?;
    let regame_info = Regex::new(r"\w+/(\w+)/send/game_info")?;
    let regame_close = Regex::new(r"\w+/(\w+)/send/game_close")?;
    let restatus = Regex::new(r"\w+/(\w+)/send/status")?;
    let rereconnect = Regex::new(r"\w+/(\w+)/send/reconnect")?;
    
    let mut sender1: Sender<SqlData> = event_room::HandleSqlRequest(pool.clone())?;
    let mut sender: Sender<RoomEventData> = event_room::init(tx, sender1.clone(), pool.clone())?;
    
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
                        let vo : serde_json::Result<Value> = serde_json::from_str(msg);
                        if reset.is_match(topic_name) {
                            info!("reset");
                            sender.send(RoomEventData::Reset());
                        }
                        let vo : serde_json::Result<Value> = serde_json::from_str(msg);
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
                                event_member::login(userid, v, pool.clone(), sender.clone(), sender1.clone())?;
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
                            } else if represtart_get.is_match(topic_name) {
                                let cap = represtart_get.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("prestart_get: userid: {} json: {:?}", userid, v);
                                event_room::prestart_get(userid, v, sender.clone())?;
                            }  else if represtart.is_match(topic_name) {
                                let cap = represtart.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("prestart: userid: {} json: {:?}", userid, v);
                                event_room::prestart(userid, v, sender.clone())?;
                            } else if releave.is_match(topic_name) {
                                let cap = releave.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("leave: userid: {} json: {:?}", userid, v);
                                event_room::leave(userid, v, sender.clone())?;
                            } else if restart_game.is_match(topic_name) {
                                let cap = restart_game.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("start_game: userid: {} json: {:?}", userid, v);
                                event_room::start_game(userid, v, sender.clone())?;
                            } else if regame_over.is_match(topic_name) {
                                let cap = regame_over.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("game_over: userid: {} json: {:?}", userid, v);
                                event_room::game_over(userid, v, sender.clone())?;
                            } else if regame_info.is_match(topic_name) {
                                let cap = regame_info.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("game_info: userid: {} json: {:?}", userid, v);
                                event_room::game_info(userid, v, sender.clone())?;
                            } else if regame_close.is_match(topic_name) {
                                let cap = regame_close.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("game_close: userid: {} json: {:?}", userid, v);
                                event_room::game_close(userid, v, sender.clone())?;
                            } else if restatus.is_match(topic_name) {
                                let cap = restatus.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("status: userid: {} json: {:?}", userid, v);
                                event_room::status(userid, v, sender.clone())?;
                            } else if rereconnect.is_match(topic_name) {
                                let cap = rereconnect.captures(topic_name).unwrap();
                                let userid = cap[1].to_string();
                                info!("reconnect: userid: {} json: {:?}", userid, v);
                                event_room::reconnect(userid, v, sender.clone())?;
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
