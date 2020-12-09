use redis::Commands;

use chrono::prelude::*;
use chrono::Duration as Cduration;
use log::{error, info, trace, warn};
use serde_derive::{Deserialize, Serialize};
use serde_json::json;
use serde_json::{self, Value};
use std::env;
use std::io::ErrorKind;
use std::io::{self, Write};
use std::panic;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use ::futures::Future;
use crossbeam_channel::{bounded, select, tick, Receiver, Sender};
use failure::Error;
use rayon::prelude::*;
use rayon::slice::*;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex, RwLock};

use crate::event_room::*;
use crate::game::*;
use crate::msg::*;
use crate::room::*;

#[derive(Clone, Debug, Default)]
pub struct RemoveGameData {
    pub game_id: u64,
}

pub enum GameingData {
    UpdateNGGame(NGGame),
    UpdateRKGame(RKGame),
    UpdateATGame(ATGame),
    RmoveNGGame(RemoveGameData),
    RmoveRKGame(RemoveGameData),
    RmoveATGame(RemoveGameData),
}

pub fn process_ng(
    msgtx: Sender<MqttMsg>,
    tx2: Sender<RoomEventData>,
    tx3: Sender<SqlData>,
    TotalUsers: BTreeMap<String, Rc<RefCell<User>>>,
    game_id: &u64,
    group: &mut Rc<RefCell<NGGame>>,
) -> Result<(), Error> {
    let mode = "ng";
    let res = group.borrow_mut().check_status();
    match res {
        NGGameStatus::Loading => {
            if group.borrow_mut().check_loading() {
                group.borrow_mut().next_status();
            }
        }
        NGGameStatus::Pick => {
            if group.borrow_mut().choose_time == NG_CHOOSE_HERO_TIME {
                msgtx.try_send(MqttMsg {
                    topic: format!("game/{}/res/game_status", game_id),
                    msg: format!(
                        r#"{{"status":"pick", "time":{}, "picker":{:?}}}"#,
                        group.borrow().choose_time.clone(),
                        group.borrow().pick_position.clone()
                    ),
                })?;
            }
            let mut isJump = false;
            for index in &group.borrow().pick_position {
                if let Some(u) = TotalUsers.get(&group.borrow().user_names[*index]) {
                    if u.borrow().hero == "" {
                        isJump = true;
                        if group.borrow().choose_time <= BUFFER {
                            let jumpData = JumpData {
                                id: u.borrow().id.clone(),
                                game: *game_id,
                                msg: "jump".to_string(),
                            };
                            tx2.try_send(RoomEventData::Jump(jumpData));
                        }
                    }
                }
            }
            let isCheck = group.borrow_mut().check_lock();
            if !isJump && (isCheck || group.borrow().choose_time <= BUFFER) {
                group.borrow_mut().next_status();
            }
            group.borrow_mut().choose_time -= 1;
        }
        NGGameStatus::ReadyToStart => {
            if group.borrow().ready_to_start_time == READY_TO_START_TIME {
                let mut isJump = false;
                for index in &group.borrow().pick_position {
                    if let Some(u) = TotalUsers.get(&group.borrow().user_names[*index]) {
                        println!("id : {}, hero : {}", u.borrow().id.clone(), u.borrow().hero.clone());
                        if u.borrow().hero == "" {
                            let jumpData = JumpData {
                                id: u.borrow().id.clone(),
                                game: *game_id,
                                msg: "jump".to_string(),
                            };
                            tx2.try_send(RoomEventData::Jump(jumpData));
                            isJump = true;
                        }
                    }
                }
                if !isJump {
                    let mut chooseData: Vec<UserChooseData> = vec![];
                    let mut values = format!("values ({}, '{}'", game_id, mode);
                    for user_id in &group.borrow().user_names {
                        if let Some(u) = TotalUsers.get(user_id) {
                            let data = UserChooseData {
                                steam_id: user_id.to_string(),
                                hero: u.borrow().hero.clone(),
                                ban_hero: u.borrow().ban_hero.clone(),
                            };
                            chooseData.push(data);
                        }
                    }
                    let sqlGameInfoData = SqlGameInfoData {
                        game: group.borrow().game_id,
                        mode: mode.to_string(),
                        chooseData: chooseData,
                    };
                    tx3.try_send(SqlData::UpdateGameInfo(sqlGameInfoData));
                    msgtx.try_send(MqttMsg {
                        topic: format!("game/{}/res/game_status", game_id),
                        msg: format!(
                            r#"{{"status":"readyToStart", "time": {}, "player":{:?}}}"#,
                            &group.borrow().ready_to_start_time,
                            &group.borrow().user_names
                        ),
                    })?;
                }
            }
            if group.borrow_mut().ready_to_start_time < 0 {
                msgtx.try_send(MqttMsg{topic:format!("game/{}/res/game_status", game_id),
                    msg: format!(r#"{{"status":"gaming", "game": {}, "player":{:?}}}"#,game_id, &group.borrow().user_names)})?;
                for id in &group.borrow().user_names {
                    tx2.try_send(RoomEventData::GameStart(GameStartData{id: id.to_string()}));
                }
                group.borrow_mut().next_status();
            }
            group.borrow_mut().ready_to_start_time -= 1;
        }
        NGGameStatus::Gaming => {
        }
        NGGameStatus::Finished => {}
    }
    Ok(())
}

pub fn process_rk(
    msgtx: Sender<MqttMsg>,
    tx2: Sender<RoomEventData>,
    tx3: Sender<SqlData>,
    TotalUsers: BTreeMap<String, Rc<RefCell<User>>>,
    game_id: &u64,
    group: &mut Rc<RefCell<RKGame>>,
) -> Result<(), Error> {
    let mode = "rk";
    let res = group.borrow_mut().check_status();
    match res {
        RKGameStatus::Loading => {
            if group.borrow_mut().check_loading() {
                group.borrow_mut().next_status();
            }
        }
        RKGameStatus::Ban => {
            if group.borrow_mut().ban_time == BAN_HERO_TIME {
                msgtx.try_send(MqttMsg {
                    topic: format!("game/{}/res/game_status", game_id),
                    msg: format!(
                        r#"{{"status":"ban", "time":{}, "picker":{:?}}}"#,
                        group.borrow().ban_time.clone(),
                        group.borrow().pick_position.clone()
                    ),
                })?;
            }
            group.borrow_mut().ban_time -= 1;
            let mut isJump = false;
            for index in &group.borrow().pick_position {
                if let Some(u) = TotalUsers.get(&group.borrow().user_names[*index]) {
                    if u.borrow().ban_hero == "" {
                        isJump = true;
                    }
                }
            }
            if group.borrow_mut().ban_time <= BUFFER || !isJump{
                group.borrow_mut().next_status();
            }
        }
        RKGameStatus::Pick => {
            if group.borrow_mut().choose_time == CHOOSE_HERO_TIME {
                msgtx.try_send(MqttMsg {
                    topic: format!("game/{}/res/game_status", game_id),
                    msg: format!(
                        r#"{{"status":"pick", "time":{}, "picker":{:?}}}"#,
                        group.borrow().choose_time.clone(),
                        group.borrow().pick_position.clone()
                    ),
                })?;
            }
            group.borrow_mut().choose_time -= 1;
            let mut isJump = false;
            for index in &group.borrow().pick_position {
                if let Some(u) = TotalUsers.get(&group.borrow().user_names[*index]) {
                    if u.borrow().hero == "" {
                        isJump = true;
                        if group.borrow().choose_time <= BUFFER {
                            let jumpData = JumpData {
                                id: u.borrow().id.clone(),
                                game: *game_id,
                                msg: "jump".to_string(),
                            };
                            tx2.try_send(RoomEventData::Jump(jumpData));
                        }
                    }
                }
            }
            if !isJump {
                group.borrow_mut().next_status();
            }
        }
        RKGameStatus::ReadyToStart => {
            if group.borrow().ready_to_start_time == READY_TO_START_TIME {
                let mut isJump = false;
                for index in &group.borrow().pick_position {
                    if let Some(u) = TotalUsers.get(&group.borrow().user_names[*index]) {
                        println!("id : {}, hero : {}", u.borrow().id.clone(), u.borrow().hero.clone());
                        if u.borrow().hero == "" {
                            let jumpData = JumpData {
                                id: u.borrow().id.clone(),
                                game: *game_id,
                                msg: "jump".to_string(),
                            };
                            tx2.try_send(RoomEventData::Jump(jumpData));
                            isJump = true;
                        }
                    }
                }
                if !isJump {
                    let mut chooseData: Vec<UserChooseData> = vec![];
                    let mut values = format!("values ({}, '{}'", game_id, mode);
                    for user_id in &group.borrow().user_names {
                        if let Some(u) = TotalUsers.get(user_id) {
                            let data = UserChooseData {
                                steam_id: user_id.to_string(),
                                hero: u.borrow().hero.clone(),
                                ban_hero: u.borrow().ban_hero.clone(),
                            };
                            chooseData.push(data);
                        }
                    }
                    let sqlGameInfoData = SqlGameInfoData {
                        game: group.borrow().game_id,
                        mode: mode.to_string(),
                        chooseData: chooseData,
                    };
                    tx3.try_send(SqlData::UpdateGameInfo(sqlGameInfoData));
                    msgtx.try_send(MqttMsg {
                        topic: format!("game/{}/res/game_status", game_id),
                        msg: format!(
                            r#"{{"status":"readyToStart", "time": {}, "player":{:?}}}"#,
                            &group.borrow().ready_to_start_time,
                            &group.borrow().user_names
                        ),
                    })?;
                }
            }
            if group.borrow_mut().ready_to_start_time < 0 {
                msgtx.try_send(MqttMsg{topic:format!("game/{}/res/game_status", game_id),
                    msg: format!(r#"{{"status":"gaming", "game": {}, "player":{:?}}}"#,game_id, &group.borrow().user_names)})?;
                for id in &group.borrow().user_names {
                    tx2.try_send(RoomEventData::GameStart(GameStartData{id: id.to_string()}));
                }
                group.borrow_mut().next_status();
            }
            group.borrow_mut().ready_to_start_time -= 1;
        }
        RKGameStatus::Gaming => {}
        RKGameStatus::Finished => {}
    }
    Ok(())
}

pub fn process_at(
    msgtx: Sender<MqttMsg>,
    tx2: Sender<RoomEventData>,
    tx3: Sender<SqlData>,
    TotalUsers: BTreeMap<String, Rc<RefCell<User>>>,
    game_id: &u64,
    group: &mut Rc<RefCell<ATGame>>,
) -> Result<(), Error> {
    let mode = "at";
    let res = group.borrow_mut().check_status();
    match res {
        ATGameStatus::Loading => {
            if group.borrow_mut().check_loading() {
                group.borrow_mut().next_status();
            }
        }
        ATGameStatus::Ban => {
            if group.borrow_mut().ban_time == BAN_HERO_TIME {
                msgtx.try_send(MqttMsg {
                    topic: format!("game/{}/res/game_status", game_id),
                    msg: format!(
                        r#"{{"status":"ban", "time":{}, "picker":{:?}}}"#,
                        group.borrow().ban_time.clone(),
                        group.borrow().pick_position.clone()
                    ),
                })?;
            }
            group.borrow_mut().ban_time -= 1;
            let mut isJump = false;
            for index in &group.borrow().pick_position {
                if let Some(u) = TotalUsers.get(&group.borrow().user_names[*index]) {
                    if u.borrow().ban_hero == "" {
                        isJump = true;
                    }
                }
            }
            if group.borrow_mut().ban_time <= BUFFER || !isJump{
                group.borrow_mut().next_status();
            }
        }
        ATGameStatus::Pick => {
            if group.borrow_mut().choose_time == CHOOSE_HERO_TIME {
                msgtx.try_send(MqttMsg {
                    topic: format!("game/{}/res/game_status", game_id),
                    msg: format!(
                        r#"{{"status":"pick", "time":{}, "picker":{:?}}}"#,
                        group.borrow().choose_time.clone(),
                        group.borrow().pick_position.clone()
                    ),
                })?;
            }
            group.borrow_mut().choose_time -= 1;
            let mut isJump = false;
            for index in &group.borrow().pick_position {
                if let Some(u) = TotalUsers.get(&group.borrow().user_names[*index]) {
                    if u.borrow().hero == "" {
                        isJump = true;
                        if group.borrow().choose_time <= BUFFER {
                            let jumpData = JumpData {
                                id: u.borrow().id.clone(),
                                game: *game_id,
                                msg: "jump".to_string(),
                            };
                            tx2.try_send(RoomEventData::Jump(jumpData));
                        }
                    }
                }
            }
            if !isJump {
                group.borrow_mut().next_status();
            }
        }
        ATGameStatus::ReadyToStart => {
            if group.borrow().ready_to_start_time == READY_TO_START_TIME {
                let mut isJump = false;
                for index in &group.borrow().pick_position {
                    if let Some(u) = TotalUsers.get(&group.borrow().user_names[*index]) {
                        println!("id : {}, hero : {}", u.borrow().id.clone(), u.borrow().hero.clone());
                        if u.borrow().hero == "" {
                            let jumpData = JumpData {
                                id: u.borrow().id.clone(),
                                game: *game_id,
                                msg: "jump".to_string(),
                            };
                            tx2.try_send(RoomEventData::Jump(jumpData));
                            isJump = true;
                        }
                    }
                }
                if !isJump {
                    let mut chooseData: Vec<UserChooseData> = vec![];
                    let mut values = format!("values ({}, '{}'", game_id, mode);
                    for user_id in &group.borrow().user_names {
                        if let Some(u) = TotalUsers.get(user_id) {
                            let data = UserChooseData {
                                steam_id: user_id.to_string(),
                                hero: u.borrow().hero.clone(),
                                ban_hero: u.borrow().ban_hero.clone(),
                            };
                            chooseData.push(data);
                        }
                    }
                    let sqlGameInfoData = SqlGameInfoData {
                        game: group.borrow().game_id,
                        mode: mode.to_string(),
                        chooseData: chooseData,
                    };
                    tx3.try_send(SqlData::UpdateGameInfo(sqlGameInfoData));
                    msgtx.try_send(MqttMsg {
                        topic: format!("game/{}/res/game_status", game_id),
                        msg: format!(
                            r#"{{"status":"readyToStart", "time": {}, "player":{:?}}}"#,
                            &group.borrow().ready_to_start_time,
                            &group.borrow().user_names
                        ),
                    })?;
                }
            }
            if group.borrow_mut().ready_to_start_time < 0 {
                msgtx.try_send(MqttMsg{topic:format!("game/{}/res/game_status", game_id),
                    msg: format!(r#"{{"status":"gaming", "game": {}, "player":{:?}}}"#,game_id, &group.borrow().user_names)})?;
                for id in &group.borrow().user_names {
                    tx2.try_send(RoomEventData::GameStart(GameStartData{id: id.to_string()}));
                }
                group.borrow_mut().next_status();
            }
            group.borrow_mut().ready_to_start_time -= 1;
        }
        ATGameStatus::Gaming => {}
        ATGameStatus::Finished => {}
    }
    Ok(())
}