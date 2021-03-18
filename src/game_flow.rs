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
    UpdateARAMGame(ARAMGame),
    RmoveNGGame(RemoveGameData),
    RmoveRKGame(RemoveGameData),
    RmoveATGame(RemoveGameData),
    RmoveARAMGame(RemoveGameData),
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
        NGGameStatus::Ban => {
            if group.borrow_mut().ban_time == BAN_HERO_TIME {
                send_ban_msg(
                    &msgtx,
                    *game_id,
                    group.borrow().ban_time.clone(),
                    group.borrow().pick_position.clone(),
                );
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
            if group.borrow_mut().ban_time <= BUFFER || !isJump {
                group.borrow_mut().rollBanHero();
                group.borrow_mut().next_status();
            }
        }
        NGGameStatus::Pick => {
            if group.borrow_mut().choose_time == NG_CHOOSE_HERO_TIME {
                send_pick_msg(
                    &msgtx,
                    *game_id,
                    group.borrow().choose_time.clone(),
                    group.borrow().pick_position.clone(),
                    group.borrow().ban_heros.clone(),
                );
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
        NGGameStatus::ReadyToStart => {
            if group.borrow().ready_to_start_time == READY_TO_START_TIME {
                let mut isJump = false;
                for index in &group.borrow().pick_position {
                    if let Some(u) = TotalUsers.get(&group.borrow().user_names[*index]) {
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
                    send_ready_to_start_msg(
                        &msgtx,
                        *game_id,
                        group.borrow().ready_to_start_time,
                        group.borrow().user_names.clone(),
                    );
                }
            }
            if group.borrow_mut().ready_to_start_time < 0 {
                send_gaming_msg(&msgtx, *game_id, group.borrow().user_names.clone());
                for id in &group.borrow().user_names {
                    tx2.try_send(RoomEventData::GameStart(GameStartData {
                        id: id.to_string(),
                    }));
                }
                group.borrow_mut().next_status();
            }
            group.borrow_mut().ready_to_start_time -= 1;
        }
        NGGameStatus::Gaming => {}
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
                send_ban_msg(
                    &msgtx,
                    *game_id,
                    group.borrow().ban_time,
                    group.borrow().pick_position.clone(),
                );
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
            if group.borrow_mut().ban_time <= BUFFER || !isJump {
                group.borrow_mut().next_status();
            }
        }
        RKGameStatus::Pick => {
            group.borrow_mut().updateBanHeros();
            if group.borrow_mut().choose_time == CHOOSE_HERO_TIME {
                send_pick_msg(
                    &msgtx,
                    *game_id,
                    group.borrow().choose_time.clone(),
                    group.borrow().pick_position.clone(),
                    group.borrow().ban_heros.clone(),
                );
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
                    send_ready_to_start_msg(
                        &msgtx,
                        *game_id,
                        group.borrow().ready_to_start_time,
                        group.borrow().user_names.clone(),
                    );
                }
            }
            if group.borrow_mut().ready_to_start_time < 0 {
                send_gaming_msg(&msgtx, *game_id, group.borrow().user_names.clone());
                for id in &group.borrow().user_names {
                    tx2.try_send(RoomEventData::GameStart(GameStartData {
                        id: id.to_string(),
                    }));
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
                send_ban_msg(
                    &msgtx,
                    *game_id,
                    group.borrow().ban_time.clone(),
                    group.borrow().pick_position.clone(),
                );
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
            if group.borrow_mut().ban_time <= BUFFER || !isJump {
                group.borrow_mut().next_status();
            }
        }
        ATGameStatus::Pick => {
            group.borrow_mut().updateBanHeros();
            if group.borrow_mut().choose_time == CHOOSE_HERO_TIME {
                send_pick_msg(
                    &msgtx,
                    *game_id,
                    group.borrow().choose_time.clone(),
                    group.borrow().pick_position.clone(),
                    group.borrow().ban_heros.clone(),
                );
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
                    send_ready_to_start_msg(
                        &msgtx,
                        *game_id,
                        group.borrow().ready_to_start_time.clone(),
                        group.borrow().user_names.clone(),
                    );
                }
            }
            if group.borrow_mut().ready_to_start_time < 0 {
                send_gaming_msg(&msgtx, *game_id, group.borrow().user_names.clone());
                for id in &group.borrow().user_names {
                    tx2.try_send(RoomEventData::GameStart(GameStartData {
                        id: id.to_string(),
                    }));
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

pub fn process_aram(
    msgtx: Sender<MqttMsg>,
    tx2: Sender<RoomEventData>,
    tx3: Sender<SqlData>,
    TotalUsers: BTreeMap<String, Rc<RefCell<User>>>,
    game_id: &u64,
    group: &mut Rc<RefCell<ARAMGame>>,
) -> Result<(), Error> {
    let mode = "aram";
    let res = group.borrow_mut().check_status();
    match res {
        ARAMGameStatus::Loading => {
            if group.borrow_mut().check_loading() {
                group.borrow_mut().next_status();
                println!("next");
            }
        }
        ARAMGameStatus::Ban => {
            send_ban_msg(&msgtx, *game_id, 0, group.borrow().pick_position.clone());
            group.borrow_mut().next_status();
            println!("next");
        }
        ARAMGameStatus::Pick => {
            group.borrow_mut().rollHeros();
            send_heros_msg(&msgtx, *game_id, group.borrow().heros.clone());
            for index in &group.borrow().pick_position {
                if let Some(u) = TotalUsers.get(&group.borrow().user_names[*index]) {
                    let mqttmsg = MqttMsg{topic:format!("member/{}/res/ng_choose_hero", u.borrow().id),
                        msg: format!(r#"{{"id":"{}", "hero":"{}"}}"#, u.borrow().id, u.borrow().hero.clone())};
                    msgtx.try_send(mqttmsg)?;
                }
            }
            group.borrow_mut().next_status();
        }
        ARAMGameStatus::ReadyToStart => {
            if group.borrow().ready_to_start_time == ARAM_READY_TO_START_TIME {
                let mut isJump = false;
                for index in &group.borrow().pick_position {
                    if let Some(u) = TotalUsers.get(&group.borrow().user_names[*index]) {
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
                    send_ready_to_start_msg(
                        &msgtx,
                        *game_id,
                        group.borrow().ready_to_start_time,
                        group.borrow().user_names.clone(),
                    );
                }
            }
            if group.borrow_mut().ready_to_start_time < 0 {
                send_gaming_msg(&msgtx, *game_id, group.borrow().user_names.clone());
                for id in &group.borrow().user_names {
                    tx2.try_send(RoomEventData::GameStart(GameStartData {
                        id: id.to_string(),
                    }));
                }
                group.borrow_mut().next_status();
            }
            group.borrow_mut().ready_to_start_time -= 1;
        }
        ARAMGameStatus::Gaming => {}
        ARAMGameStatus::Finished => {}
    }
    Ok(())
}

fn send_ban_msg(
    msgtx: &Sender<MqttMsg>,
    game_id: u64,
    ban_time: i16,
    pick_position: Vec<usize>,
) -> Result<(), Error> {
    msgtx.try_send(MqttMsg {
        topic: format!("game/{}/res/game_status", game_id),
        msg: format!(
            r#"{{"status":"ban", "time":{}, "picker":{:?}}}"#,
            ban_time, pick_position,
        ),
    })?;
    Ok(())
}

fn send_pick_msg(
    msgtx: &Sender<MqttMsg>,
    game_id: u64,
    pick_time: i16,
    pick_position: Vec<usize>,
    ban_heros: Vec<Vec<String>>,
) -> Result<(), Error> {
    msgtx.try_send(MqttMsg {
        topic: format!("game/{}/res/game_status", game_id),
        msg: format!(
            r#"{{"status":"pick", "time":{}, "picker":{:?}, "ban":{:?}}}"#,
            pick_time, pick_position, ban_heros
        ),
    })?;
    Ok(())
}

fn send_ready_to_start_msg(
    msgtx: &Sender<MqttMsg>,
    game_id: u64,
    ready_to_start_time: i16,
    user_names: Vec<String>,
) -> Result<(), Error> {
    msgtx.try_send(MqttMsg {
        topic: format!("game/{}/res/game_status", game_id),
        msg: format!(
            r#"{{"status":"readyToStart", "time": {}, "player":{:?}}}"#,
            ready_to_start_time, user_names,
        ),
    })?;
    Ok(())
}

fn send_gaming_msg(
    msgtx: &Sender<MqttMsg>,
    game_id: u64,
    user_names: Vec<String>,
) -> Result<(), Error> {
    msgtx.try_send(MqttMsg {
        topic: format!("game/{}/res/game_status", game_id),
        msg: format!(
            r#"{{"status":"gaming", "game": {}, "player":{:?}}}"#,
            game_id, user_names,
        ),
    })?;
    Ok(())
}

fn send_heros_msg(msgtx: &Sender<MqttMsg>, game_id: u64, heros: Vec<String>) -> Result<(), Error> {
    msgtx.try_send(MqttMsg {
        topic: format!("game/{}/res/heros", game_id),
        msg: format!(r#"{{"heros":{:?}}}"#, heros,),
    })?;
    Ok(())
}
