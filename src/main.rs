#![allow(warnings)]
use log::{info, warn, error, trace};

mod event_member;
mod event_room;
mod game;
mod game_flow;
mod room;
mod msg;
mod elo;

use std::cell::RefCell;
use std::rc::Rc;
use std::env;
use std::io::Write;
use failure::Error;
use std::net::TcpStream;
use std::str;
use clap::{App, Arg};
use uuid::Uuid;
use rumqtt::{MqttClient, MqttOptions, QoS, ReconnectOptions};

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
use crate::event_room::QueueData;
use crate::msg::*;

fn generate_client_id() -> String {
    let s = format!("Elo_Pub_{}", Uuid::new_v4());
    (&s[..16]).to_string()
}

fn get_url() -> String {
    "mysql://root:sonicrain2020@172.104.72.206:4000/nobu".into()
    // "mysql://elo:elo@localhost:3306/nobu".into() //test
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
        ).arg(
            Arg::with_name("BACKUP")
            .short("b")
            .long("backup")
            .takes_value(true)
            .help("backup"),
        ).get_matches();

    let server_addr = matches.value_of("SERVER").unwrap_or("172.104.72.206").to_owned();
    // let server_addr = matches.value_of("SERVER").unwrap_or("172.105.232.176").to_owned(); //test
    let server_port = matches.value_of("PORT").unwrap_or("1883").to_owned();
    let client_id = matches
        .value_of("CLIENT_ID")
        .map(|x| x.to_owned())
        .unwrap_or("Elo test Server".to_owned());
    let mut isBackup: bool = matches.value_of("BACKUP").unwrap_or("false").to_owned().parse()?;
    println!("Backup: {}", isBackup);
    let mut mqtt_options = MqttOptions::new(client_id.as_str(), server_addr.as_str(), server_port.parse::<u16>()?);
    mqtt_options = mqtt_options.set_keep_alive(100);
    mqtt_options = mqtt_options.set_request_channel_capacity(10000);
    mqtt_options = mqtt_options.set_notification_channel_capacity(10000);
    let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options.clone())?;
    
    // Server message
    mqtt_client.subscribe("server/+/res/heartbeat", QoS::AtMostOnce)?;
    mqtt_client.subscribe("server/send/control", QoS::AtMostOnce)?;//doc server.drawio
    mqtt_client.subscribe("server/send/check_state", QoS::AtMostOnce)?;//doc server.drawio
    mqtt_client.subscribe("server/send/free", QoS::AtMostOnce)?;
    mqtt_client.subscribe("server/send/system_ban", QoS::AtMostOnce)?;
    mqtt_client.subscribe("server/send/update_heros", QoS::AtMostOnce)?;
    // Client message
    mqtt_client.subscribe("member/+/send/login", QoS::AtMostOnce)?;//doc login.drawio
    mqtt_client.subscribe("member/+/send/logout", QoS::AtMostOnce)?;//doc login.drwio
    mqtt_client.subscribe("member/+/send/ng_choose_hero", QoS::AtMostOnce)?;//doc choose_hero
    mqtt_client.subscribe("member/+/send/status", QoS::AtMostOnce)?;//doc login.drwio
    mqtt_client.subscribe("member/+/send/reconnect", QoS::AtMostOnce)?;//doc reconnect.drwio
    mqtt_client.subscribe("member/+/send/add_black_list", QoS::AtMostOnce)?;//doc black_list.drawio
    mqtt_client.subscribe("member/+/send/rm_black_list", QoS::AtMostOnce)?;//doc black_list.drawio
    mqtt_client.subscribe("member/+/send/query_black_list", QoS::AtMostOnce)?;//doc black_list.drawio
    mqtt_client.subscribe("member/+/send/check_restriction", QoS::AtMostOnce)?;//doc check_state
    mqtt_client.subscribe("member/+/send/check_in_game", QoS::AtMostOnce)?;//doc check_state
    mqtt_client.subscribe("member/+/send/leave_game", QoS::AtMostOnce)?;//doc game
    mqtt_client.subscribe("member/+/send/get_game_historys", QoS::AtMostOnce)?;//doc get_game_historys
    mqtt_client.subscribe("member/+/send/get_buy_historys", QoS::AtMostOnce)?;
    mqtt_client.subscribe("member/+/send/binding", QoS::AtMostOnce)?;
    mqtt_client.subscribe("member/+/send/check_binding", QoS::AtMostOnce)?;
    mqtt_client.subscribe("member/+/send/get_leaderboard", QoS::AtMostOnce)?;
    mqtt_client.subscribe("member/+/send/check_state", QoS::AtMostOnce)?;
    mqtt_client.subscribe("member/+/send/check_room", QoS::AtMostOnce)?;
    mqtt_client.subscribe("member/+/send/buy_good", QoS::AtMostOnce)?;
    mqtt_client.subscribe("member/+/send/get_good", QoS::AtMostOnce)?;

    mqtt_client.subscribe("room/+/send/create", QoS::AtMostOnce)?;//doc room
    mqtt_client.subscribe("room/+/send/close", QoS::AtMostOnce)?;//doc room
    mqtt_client.subscribe("room/+/send/start_queue", QoS::AtMostOnce)?;//doc room
    mqtt_client.subscribe("room/+/send/cancel_queue", QoS::AtMostOnce)?;//doc room
    mqtt_client.subscribe("room/+/send/invite", QoS::AtMostOnce)?;//doc room
    mqtt_client.subscribe("room/+/send/join", QoS::AtMostOnce)?;//doc room
    mqtt_client.subscribe("room/+/send/reject", QoS::AtMostOnce)?;//doc room
    mqtt_client.subscribe("room/+/send/leave", QoS::AtMostOnce)?;//doc room
    mqtt_client.subscribe("room/+/send/ready", QoS::AtMostOnce)?;//doc room
    mqtt_client.subscribe("room/+/send/start_get", QoS::AtMostOnce)?;//doc room

    mqtt_client.subscribe("game/+/send/game_close", QoS::AtMostOnce)?;//dpc game
    mqtt_client.subscribe("game/+/send/game_over", QoS::AtMostOnce)?;//doc game
    mqtt_client.subscribe("game/+/send/game_info", QoS::AtMostOnce)?;//doc game
    mqtt_client.subscribe("game/+/send/start_game", QoS::AtMostOnce)?;//doc game
    mqtt_client.subscribe("game/+/send/set_password", QoS::AtMostOnce)?;//doc game
    mqtt_client.subscribe("game/+/send/loading", QoS::AtMostOnce)?;
    mqtt_client.subscribe("game/+/send/locked_hero", QoS::AtMostOnce)?;
    mqtt_client.subscribe("game/+/send/jump", QoS::AtMostOnce)?;
    mqtt_client.subscribe("game/+/send/ban_hero", QoS::AtMostOnce)?;
    mqtt_client.subscribe("game/+/send/get_heros", QoS::AtMostOnce)?;
    mqtt_client.subscribe("game/+/send/try_swap_hero", QoS::AtMostOnce)?;
    mqtt_client.subscribe("game/+/send/swap_hero", QoS::AtMostOnce)?;
    let mut isServerLive = true;
    
    
    let (tx, rx):(Sender<MqttMsg>, Receiver<MqttMsg>) = bounded(10000);
    let pool = mysql::Pool::new(get_url().as_str())?;
    let redis_client = redis::Client::open("redis://127.0.0.1:6379/1")?;
    thread::sleep_ms(100);
    
    for _ in 0..8 {
        let server_addr = server_addr.clone();
        let server_port = server_port.clone();
        let rx1 = rx.clone();
        
        thread::spawn(move || -> Result<(), Error> {
            
            let mut mqtt_options = MqttOptions::new(generate_client_id(), server_addr, server_port.parse::<u16>()?);
            mqtt_options = mqtt_options.set_keep_alive(100);
            mqtt_options = mqtt_options.set_request_channel_capacity(10000);
            mqtt_options = mqtt_options.set_notification_channel_capacity(10000);
            //mqtt_options = mqtt_options.set_reconnect_opts(ReconnectOptions::Always(1));
            println!("mqtt_options {:#?}", mqtt_options);
            let update = tick(Duration::from_millis(1000));
            let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options.clone())?;
            loop {
                select! {
                    recv(rx1) -> d => {
                        let handle = || -> Result<(), Error> 
                        {
                            if let Ok(d) = d {
                                if d.topic == "server/0/res/dead" {
                                    isServerLive = false;
                                    isBackup = false;
                                }
                                if d.topic.len() > 2 {
                                    let msg_res = mqtt_client.publish(d.topic, QoS::AtMostOnce, false, d.msg);
                                    match msg_res {
                                        Ok(_) =>{},
                                        Err(x) => {
                                            panic!("??? {}", x);
                                        }
                                    }
                                }
                            }
                            Ok(())
                        };
                        
                        if let Err(msg) = handle() {
                            panic!("mqtt {:?}", msg);
                            let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options.clone())?;
                        }
                    }
                }
            }
            
            Ok(())
        });
    }
    
    let server = Regex::new(r"\w+/(\w+)/res/(\w+)")?;
    let check_server = tick(Duration::from_millis(1000));
    
    let readd_black_list = Regex::new(r"\w+/(\w+)/send/add_black_list")?;
    let rerm_black_list = Regex::new(r"\w+/(\w+)/send/rm_black_list")?;
    let requery_black_list = Regex::new(r"\w+/(\w+)/send/query_black_list")?;
    let relocked_hero = Regex::new(r"\w+/(\w+)/send/locked_hero")?;
    let relogin = Regex::new(r"\w+/(\w+)/send/login")?;
    let relogout = Regex::new(r"\w+/(\w+)/send/logout")?;
    let recreate = Regex::new(r"\w+/(\w+)/send/create")?;
    let reclose = Regex::new(r"\w+/(\w+)/send/close")?;
    let restart_queue = Regex::new(r"\w+/(\w+)/send/start_queue")?;
    let recancel_queue = Regex::new(r"\w+/(\w+)/send/cancel_queue")?;
    let restart_get = Regex::new(r"\w+/(\w+)/send/start_get")?;
    let reready = Regex::new(r"\w+/(\w+)/send/ready")?;
    let reinvite = Regex::new(r"\w+/(\w+)/send/invite")?;
    let rejoin = Regex::new(r"\w+/(\w+)/send/join")?;
    let recheck_room = Regex::new(r"\w+/(\w+)/send/check_room")?;
    let rebuy_good = Regex::new(r"\w+/(\w+)/send/buy_good")?;
    let reget_good = Regex::new(r"\w+/(\w+)/send/get_good")?;
    let rereject = Regex::new(r"\w+/(\w+)/send/reject")?;
    let reset = Regex::new(r"reset")?;
    let rechoose_hero = Regex::new(r"\w+/(\w+)/send/ng_choose_hero")?;
    let reban_hero = Regex::new(r"\w+/(\w+)/send/ban_hero")?;
    let reget_hero = Regex::new(r"\w+/(\w+)/send/get_heros")?;
    let reswap_hero = Regex::new(r"\w+/(\w+)/send/swap_hero")?;
    let releave = Regex::new(r"\w+/(\w+)/send/leave")?;
    let restart_game = Regex::new(r"\w+/(\w+)/send/start_game")?;
    let repassword = Regex::new(r"\w+/(\w+)/send/set_password")?;
    let regame_over = Regex::new(r"\w+/(\w+)/send/game_over")?;
    let regame_info = Regex::new(r"\w+/(\w+)/send/game_info")?;
    let regame_close = Regex::new(r"\w+/(\w+)/send/game_close")?;
    let restatus = Regex::new(r"\w+/(\w+)/send/status")?;
    let rereconnect = Regex::new(r"\w+/(\w+)/send/reconnect")?;
    let rejump = Regex::new(r"\w+/(\w+)/send/jump")?;
    let recheck_restriction = Regex::new(r"\w+/(\w+)/send/check_restriction")?;
    let recheckInGame = Regex::new(r"\w+/(\w+)/send/check_in_game")?;
    let releave_game = Regex::new(r"\w+/(\w+)/send/leave_game")?;
    let regetGameHistorys = Regex::new(r"\w+/(\w+)/send/get_game_historys")?;
    let regetBuyHistorys = Regex::new(r"\w+/(\w+)/send/get_buy_historys")?;
    let rebinding = Regex::new(r"\w+/(\w+)/send/binding")?;
    let recheckBinding = Regex::new(r"\w+/(\w+)/send/check_binding")?;
    let regetLeaderboard = Regex::new(r"\w+/(\w+)/send/get_leaderboard")?;
    let recontrol = Regex::new(r"\w+/send/control")?;
    let recheck_state = Regex::new(r"\w+/(\w+)/send/check_state")?;
    let reloading = Regex::new(r"\w+/(\w+)/send/loading")?;
    let refree = Regex::new(r"\w+/send/free")?;
    let resystem_ban = Regex::new(r"\w+/send/system_ban")?;
    let reupdate_heros = Regex::new(r"\w+/send/update_heros")?;
    
    
    //let mut QueueSender: Sender<QueueData>;
    let mut sender1: Sender<SqlData> = event_room::HandleSqlRequest(pool.clone())?;
    let (mut sender, mut QueueSender): (Sender<RoomEventData>, Sender<QueueData>) = event_room::init(tx.clone(), sender1.clone(), pool.clone(), redis_client.clone(), None, isBackup)?;
    let update = tick(Duration::from_millis(500));
    let mut is_live = true;
    let mut sender = sender.clone();
    let mut QueueSender = QueueSender.clone();
    
    loop {
        use rumqtt::Notification::Publish;
        
        select! {
            
            recv (check_server) -> _ => {
                if isBackup {
                    //println!("isServerLive {}", isServerLive);
                    if isServerLive == true {
                        isServerLive = false;
                    }
                    else {
                        println!("Main Server dead!!");
                        event_room::server_dead("0".to_string(), sender.clone())?;
                        isBackup = false;
                    }
                }
            },
            recv(notifications) -> notification => {
                if !is_live{
                    println!("Reconnect!");
                    
                    let (mut sender1, mut QueueSender1): (Sender<RoomEventData>, Sender<QueueData>) = event_room::init(tx.clone(), sender1.clone(), pool.clone(), redis_client.clone(), None, isBackup)?;
                    sender = sender1.clone();
                    QueueSender = QueueSender1.clone();

                    thread::sleep(Duration::from_millis(2000));
                    
                    println!("Refresh tx len: {}, queue len: {}", sender.len(), QueueSender.len());
                }
                let handle = || -> Result<(), Error> {
                    
                    if let Ok(x) = notification {
                        if let Publish(x) = x {                         
                            let payload = x.payload;
                            let msg = match str::from_utf8(&payload[..]) {
                                Ok(msg) => msg,
                                Err(err) => {
                                    return Err(failure::err_msg(format!("Failed to decode publish message {:?}", err)));
                                    //continue;
                                }
                            };
                            let topic_name = x.topic_name.as_str();
                            // println!("topic_name {}", topic_name);
                            let vo : serde_json::Result<Value> = serde_json::from_str(msg);
                            if server.is_match(topic_name) {
                                //println!("topic: {}", topic_name);
                                isServerLive = true;
                            }
                            if reset.is_match(topic_name) {
                                //info!("reset");
                                sender.send(RoomEventData::Reset());
                            }
                            let vo : serde_json::Result<Value> = serde_json::from_str(msg);
                            if let Ok(v) = vo {
                                if !recheckInGame.is_match(topic_name){
                                    info!("topic_name : {}", topic_name);
                                }
                                if reinvite.is_match(topic_name) {
                                    let cap = reinvite.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("invite: userid: {} json: {:?}", userid, v);
                                    event_room::invite(userid, v, sender.clone())?;
                                } else if rechoose_hero.is_match(topic_name) {
                                    let cap = rechoose_hero.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("choose ng hero: userid: {} json: {:?}", userid, v);
                                    event_room::choose_ng_hero(userid, v, sender.clone())?;
                                } else if reban_hero.is_match(topic_name) {
                                    let cap = reban_hero.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("ban hero: userid: {} json: {:?}", userid, v);
                                    event_room::ban_hero(userid, v, sender.clone())?;
                                } else if reget_hero.is_match(topic_name) {
                                    let cap = reget_hero.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("get heros: userid: {} json: {:?}", userid, v);
                                    event_room::get_heros(userid, v, sender.clone())?;
                                } else if reswap_hero.is_match(topic_name) {
                                    let cap = reswap_hero.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("swap hero: userid: {} json: {:?}", userid, v);
                                    event_room::swap_hero(userid, v, sender.clone())?;
                                } else if relocked_hero.is_match(topic_name) {
                                    let cap = relocked_hero.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("choose lock hero: userid: {} json: {:?}", userid, v);
                                    event_room::lock_hero(userid, v, sender.clone())?;
                                } else if rejoin.is_match(topic_name) {
                                    let cap = rejoin.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("join: userid: {} json: {:?}", userid, v);
                                    event_room::join(userid, v, sender.clone())?;
                                } else if recheck_room.is_match(topic_name) {
                                    let cap = recheck_room.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("recheck_room: userid: {} json: {:?}", userid, v);
                                    event_room::check_room(userid, v, sender.clone())?;
                                } else if rebuy_good.is_match(topic_name) {
                                    let cap = rebuy_good.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("rebuy_good: userid: {} json: {:?}", userid, v);
                                    event_room::buy_good(userid, v, sender.clone())?;
                                } else if reget_good.is_match(topic_name) {
                                    let cap = reget_good.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("reget_good: userid: {} json: {:?}", userid, v);
                                    event_room::get_good(userid, v, sender.clone())?;
                                } else if rereject.is_match(topic_name) {
                                    let cap = rereject.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("join: userid: {} json: {:?}", userid, v);
                                    event_room::reject(userid, v, sender.clone())?;
                                } else if rejump.is_match(topic_name) {
                                    let cap = rejump.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("jump: userid: {} json: {:?}", userid, v);
                                    event_room::jump(userid, v, sender.clone())?;
                                } else if recheck_restriction.is_match(topic_name) {
                                    let cap = recheck_restriction.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("check_restriction: userid: {} json: {:?}", userid, v);
                                    event_room::checkRestriction(userid, v, sender.clone())?;
                                } else if recheckInGame.is_match(topic_name) {
                                    let cap = recheckInGame.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("checkInGame: userid: {} json: {:?}", userid, v);
                                    event_room::checkInGame(userid, v, sender.clone())?;
                                } else if releave_game.is_match(topic_name) {
                                    let cap = releave_game.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("leave_game: userid: {} json: {:?}", userid, v);
                                    event_room::leaveGame(userid, v, sender.clone())?;
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
                                } else if readd_black_list.is_match(topic_name){
                                    let cap = readd_black_list.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    event_member::AddBlackList(userid, v, pool.clone(), tx.clone());
                                } else if rerm_black_list.is_match(topic_name){
                                    let cap = rerm_black_list.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    event_member::RemoveBlackList(userid, v, pool.clone(), tx.clone());
                                } else if requery_black_list.is_match(topic_name){
                                    let cap = requery_black_list.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    event_member::QueryBlackList(userid, v, pool.clone(), tx.clone());
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
                                } else if restart_get.is_match(topic_name) {
                                    let cap = restart_get.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("prestart_get: userid: {} json: {:?}", userid, v);
                                    event_room::start_get(userid, v, sender.clone())?;
                                } else if reready.is_match(topic_name) {
                                    let cap = reready.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("prestart: userid: {} json: {:?}", userid, v);
                                    event_room::ready(userid, v, sender.clone())?;
                                } else if reloading.is_match(topic_name) {
                                    let cap = reloading.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("loading: userid: {} json: {:?}", userid, v);
                                    event_room::loading(userid, v, sender.clone())?;
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
                                } else if repassword.is_match(topic_name) {
                                    let cap = repassword.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    info!("start_game: userid: {} json: {:?}", userid, v);
                                    event_room::set_password(userid, v, sender.clone())?;
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
                                } else if recontrol.is_match(topic_name) {
                                    info!("recontrol: json: {:?}", v);
                                    event_room::control(v, sender.clone())?;
                                } else if recheck_state.is_match(topic_name) {
                                    info!("recheck_state: json: {:?}", v);
                                    event_room::checkState(v, sender.clone())?;
                                } else if refree.is_match(topic_name) {
                                    info!("refree: json: {:?}", v);
                                    event_room::free(v, sender.clone())?;
                                } else if resystem_ban.is_match(topic_name) {
                                    info!("resystem_ban: json: {:?}", v);
                                    event_room::systemBan(v, sender.clone())?;
                                } else if reupdate_heros.is_match(topic_name) {
                                    info!("reupdate_heros: json: {:?}", v);
                                    event_room::updateHeros(v, sender.clone())?;
                                } else if regetGameHistorys.is_match(topic_name) {
                                    info!("regetGameHistorys: json: {:?}", v);
                                    let cap = regetGameHistorys.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    event_member::GetGameHistorys(userid, v, pool.clone(), tx.clone())?;
                                } else if regetBuyHistorys.is_match(topic_name) {
                                    info!("regetBuyHistoryselse: json: {:?}", v);
                                    let cap = regetBuyHistorys.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    event_member::GetBuyHistorys(userid, v, pool.clone(), tx.clone())?;
                                } else if rebinding.is_match(topic_name) {
                                    info!("rebinding: json: {:?}", v);
                                    let cap = rebinding.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    event_member::Binding(userid, v, pool.clone(), tx.clone())?;
                                } else if recheckBinding.is_match(topic_name) {
                                    info!("recheckBinding: json: {:?}", v);
                                    let cap = recheckBinding.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    event_member::CheckBinding(userid, v, pool.clone(), tx.clone())?;
                                } else if regetLeaderboard.is_match(topic_name) {
                                    info!("regetLeaderboard: json: {:?}", v);
                                    let cap = regetLeaderboard.captures(topic_name).unwrap();
                                    let userid = cap[1].to_string();
                                    event_member::GetLeaderboard(userid, v, pool.clone(), tx.clone())?;
                                }
                            } else {
                                warn!("Json Parser error");
                            };
                        }
                    }
                    Ok(())
                };
                if let Err(msg) = handle() {
                    println!("{:?}", msg);
                }
            }
        }
    }
    Ok(())
}
