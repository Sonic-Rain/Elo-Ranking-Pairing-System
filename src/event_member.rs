use crate::msg::*;
use failure::Error;
use log::{error, info, trace, warn};
use rumqtt::{MqttClient, MqttOptions, QoS, ReconnectOptions};
use serde_derive::{Deserialize, Serialize};
use serde_json::{self, Result, Value};
use std::env;
use std::io::{self, Write};
use std::thread;

use crate::event_room::*;
use crate::room::User;
use ::futures::Future;
use crossbeam_channel::{bounded, select, tick, Receiver, Sender};
use mysql;

#[derive(Serialize, Deserialize)]
struct LoginData {
    id: String,
}

#[derive(Serialize, Deserialize)]
struct LogoutData {
    id: String,
}

#[derive(Serialize, Deserialize)]
struct AddBlackListData {
    id: String,
    black: String,
}

#[derive(Serialize, Deserialize)]
struct RemoveBlackListData {
    id: String,
    black: String,
}

#[derive(Serialize, Deserialize)]
struct QueryBlackListData {
    id: String,
}

#[derive(Serialize, Deserialize)]
struct QueryBlackListMsg {
    list: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct GetGameHistorysData {
    id: String,
}

#[derive(Serialize, Deserialize)]
struct GameHistoryData {
    gameId: String,
    steamId: String,
    hero: String,
    level: u16,
    isWin: bool,
    mode: String,
    k: u16,
    d: u16,
    a: u16,
    cs: u16,
    money: u32,
    playTime: u16,
    date: String,
    items: Vec<String>,
}

pub fn login(
    id: String,
    v: Value,
    pool: mysql::Pool,
    sender: Sender<RoomEventData>,
    sender1: Sender<SqlData>,
) -> std::result::Result<(), Error> {
    let data: LoginData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn()?;
    let sql = format!(r#"select ng, rk, at, name from user where id='{}';"#, id);
    let qres2: mysql::QueryResult = conn.query(sql.clone())?;
    let mut ng: i16 = 0;
    let mut rk: i16 = 0;
    let mut at: i16 = 0;
    let mut name: String = "".to_owned();
    let mut count = 0;
    for row in qres2 {
        count += 1;
        let a = row?.clone();
        if let Some(n) = a.get("ng"){
            ng = mysql::from_value(n);
        } else {
            ng = 1200;
        }
        if let Some(n) = a.get("rk"){
            rk = mysql::from_value(n);
        } else {
            rk = 1200;
        }
        if let Some(n) = a.get("at"){
            at = mysql::from_value(n);
        } else {
            at = 1200;
        }
        if let Some(n) = a.get("name"){
            name = mysql::from_value(n);
        } else {
            name = "".to_string();
        }
        // ng = mysql::from_value(a.get("ng").unwrap());
        // rk = mysql::from_value(a.get("rk").unwrap());
        // at = mysql::from_value(a.get("at").unwrap());
        // name = mysql::from_value(a.get("name").unwrap());
        break;
    }
    //查無此人 建立表
    if count == 0 {
        let mut sql = format!(
            "replace into user (id, name, status, hero) values ('{}', '{}', 'online', '');",
            id, data.id
        );
        conn.query(sql.clone())?;
        sender1.send(SqlData::Login(SqlLoginData {
            id: id.clone(),
            name: name.clone(),
        }));
        ng = 1200;
        rk = 1200;
        at = 1200;
        //sender.send(RoomEventData::Login(UserLoginData {u: User { id: id.clone(), hero: name.clone(), online: true, ng: 1000, rk: 1000, ..Default::default()}}));
    }

    let qres = conn.query(format!(
        "update user set status='online' where id='{}';",
        id
    ));
    let publish_packet = match qres {
        Ok(_) => {
            //sender.send(RoomEventData::Login(UserLoginData {u: User { id: id.clone(), ng: ng, rk: rk}}));
        }
        _ => {}
    };
    if count != 0 {
        //sender.send(RoomEventData::Login(UserLoginData {u: User { id: id.clone(), hero: "default name".to_string(), online: true, ng: ng, rk: rk, ..Default::default()}, dataid: id}));
    }
    sender.send(RoomEventData::Login(UserLoginData {
        u: User {
            id: id.clone(),
            name: "default name".to_string(),
            online: true,
            ng: ng,
            rk: rk,
            at: at,
            ..Default::default()
        },
        dataid: id,
    }));
    Ok(())
}

pub fn logout(
    id: String,
    v: Value,
    pool: mysql::Pool,
    sender: Sender<RoomEventData>,
) -> std::result::Result<(), Error> {
    let mut conn = pool.get_conn()?;
    let qres = conn.query(format!(
        "update user set status='offline' where id='{}';",
        id
    ));
    let publish_packet = match qres {
        Ok(_) => {
            // sender.send(RoomEventData::Logout(UserLogoutData { id: id}));
        }
        _ => {}
    };
    sender.send(RoomEventData::Logout(UserLogoutData { id: id }));
    Ok(())
}

pub fn AddBlackList(
    id: String,
    v: Value,
    pool: mysql::Pool,
    msgtx: Sender<MqttMsg>,
) -> std::result::Result<(), Error> {
    let data: AddBlackListData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn()?;
    let mut sql = format!(
        r#"replace into black_list (user, black) values ('{}', {});"#,
        data.id, data.black
    );
    let qres = conn.query(sql);
    msgtx.try_send(MqttMsg {
        topic: format!("member/{}/res/add_black_list", id),
        msg: format!(r#"{{"msg":"added"}}"#),
    })?;
    Ok(())
}

pub fn RemoveBlackList(
    id: String,
    v: Value,
    pool: mysql::Pool,
    msgtx: Sender<MqttMsg>,
) -> std::result::Result<(), Error> {
    let data: RemoveBlackListData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn()?;
    let mut sql = format!(
        r#"delete from black_list where user={} and black={};"#,
        id, data.black
    );
    let qres = conn.query(sql);
    msgtx.try_send(MqttMsg {
        topic: format!("member/{}/res/rm_black_list", id),
        msg: format!(r#"{{"msg":"removed"}}"#),
    })?;
    Ok(())
}

pub fn QueryBlackList(
    id: String,
    v: Value,
    pool: mysql::Pool,
    msgtx: Sender<MqttMsg>,
) -> std::result::Result<(), Error> {
    let data: QueryBlackListData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn()?;
    let mut sql = format!(r#"select black from black_list where user={};"#, id);
    let qres: mysql::QueryResult = conn.query(sql.clone())?;
    let mut blackid: String;
    let mut list = Vec::new();
    for row in qres {
        let a = row?.clone();
        if let Some(n) = a.get("black") {
            blackid = mysql::from_value(n);
            list.push(blackid);
        }
        // blackid = mysql::from_value(a.get("black").unwrap());
    }
    msgtx.try_send(MqttMsg {
        topic: format!("member/{}/res/query_black_list", id),
        msg: format!(r#"{{"list":{}}}"#, serde_json::to_value(list)?),
    })?;
    Ok(())
}

pub fn GetGameHistorys(
    id: String,
    v: Value,
    pool: mysql::Pool,
    msgtx: Sender<MqttMsg>,
) -> std::result::Result<(), Error> {
    let data: GetGameHistorysData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn()?;
    let mut sql = format!(
        r#"select * from Finished_detail as d inner join Finished_game as g WHERE d.steam_id={} AND g.game_id=d.game_id ORDER BY d.game_id DESC LIMIT 20;"#,
        id
    );
    println!("{}",sql);
    let qres: mysql::QueryResult = conn.query(sql.clone())?;
    let mut gameHistorysData: Vec<GameHistoryData> = Vec::new();
    for row in qres {
        let a = row?.clone();
        let isWin: bool;
        let res: String = mysql::from_value(a.get("res").unwrap());
        if res == "W" {
            isWin = true;
        } else {
            isWin = false;
        }
        let mut items: Vec<String> = Vec::new();
        items.push(mysql::from_value(a.get("equ_1").unwrap()));
        items.push(mysql::from_value(a.get("equ_2").unwrap()));
        items.push(mysql::from_value(a.get("equ_3").unwrap()));
        items.push(mysql::from_value(a.get("equ_4").unwrap()));
        items.push(mysql::from_value(a.get("equ_5").unwrap()));
        items.push(mysql::from_value(a.get("equ_6").unwrap()));
        let mut mode = String::from("ng");
        if let Some(n) = a.get("mode") {
            if let Some(m) = mysql::from_value(n) {
                mode = m;
            }
        }
        let gameHistory = GameHistoryData {
            gameId: mysql::from_value(a.get("game_id").unwrap()), 
            steamId: mysql::from_value(a.get("steam_id").unwrap()), 
            hero: mysql::from_value(a.get("hero").unwrap()),
            mode: mode,
            level: mysql::from_value(a.get("level").unwrap()),
            isWin: isWin,
            k: mysql::from_value(a.get("k").unwrap()),
            d: mysql::from_value(a.get("d").unwrap()),
            a: mysql::from_value(a.get("a").unwrap()),
            cs: mysql::from_value(a.get("killed_unit").unwrap()),
            money: mysql::from_value(a.get("income").unwrap()),
            playTime: mysql::from_value(a.get("play_time").unwrap()),
            date: mysql::from_value(a.get("createtime").unwrap()),
            items: items,
        };
        gameHistorysData.push(gameHistory);
    }
    msgtx.try_send(MqttMsg {
        topic: format!("member/{}/res/get_game_historys", id),
        msg: serde_json::to_string(&gameHistorysData).unwrap(),
    })?;
    Ok(())
}
