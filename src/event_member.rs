use crate::msg::*;
use failure::Error;
use std::io::ErrorKind;
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
struct GetBuyHistorysData {
    id: String,
}

#[derive(Serialize, Deserialize)]
struct BindingData {
    id: String,
    steam_id: String,
    token: String,
}

#[derive(Serialize, Deserialize)]
struct CheckBindingData {
    steam_id: String
}

#[derive(Serialize, Deserialize, Debug, Default)]
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

#[derive(Serialize, Deserialize, Debug, Default)]
struct BuyHistoryData {
    steamId: String,
    name: String,
    kind: String,
    imageURL: String,
    description: String,
    sn: String,
    date: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
struct ScoreData {
    steamID: String,
    score: u16,
    win: u64,
}

#[derive(Serialize, Deserialize)]
struct LeaderboardData {
    rkLeaderboard: Vec<ScoreData>,
    atLeaderboard: Vec<ScoreData>,
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
        } else {
            warn!("mysql error: {}, line  : {}", sql, line!());
        }
    }
    msgtx.try_send(MqttMsg {
        topic: format!("member/{}/res/query_black_list", id),
        msg: format!(r#"{{"list":{}}}"#, serde_json::to_value(list)?),
    })?;
    Ok(())
}

pub fn GetBuyHistorys(
    id: String,
    v: Value,
    pool: mysql::Pool,
    msgtx: Sender<MqttMsg>,
) -> std::result::Result<(), Error> {
    let data: GetBuyHistorysData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn()?;
    let mut sql = format!(
        r#"select * from Items where steam_id='{}' order by date DESC;"#,
        id
    );
    println!("{}",sql);
    let qres: mysql::QueryResult = conn.query(sql.clone())?;
    let mut buyHistorysData: Vec<BuyHistoryData> = Vec::new();
    for row in qres {
        let a = row?.clone();
        let buyHistory = BuyHistoryData {
            steamId: mysql::from_value_opt(a.get("steam_id").ok_or(Error::from(core::fmt::Error))?)?,
            name: mysql::from_value_opt(a.get("name").ok_or(Error::from(core::fmt::Error))?)?,
            kind: mysql::from_value_opt(a.get("kind").ok_or(Error::from(core::fmt::Error))?)?,
            imageURL: mysql::from_value_opt(a.get("imageURL").ok_or(Error::from(core::fmt::Error))?)?,
            description: mysql::from_value_opt(a.get("description").ok_or(Error::from(core::fmt::Error))?)?,
            sn: mysql::from_value_opt(a.get("sn").ok_or(Error::from(core::fmt::Error))?)?,
            date: mysql::from_value_opt(a.get("date").ok_or(Error::from(core::fmt::Error))?)?,
        };
        buyHistorysData.push(buyHistory);
    }
    msgtx.try_send(MqttMsg {
        topic: format!("member/{}/res/get_buy_historys", id),
        msg: serde_json::to_string(&buyHistorysData)?,
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
        let mut isWin: bool;
        let mut res: String;
        if let Some(r) = a.get("res") {
            res = mysql::from_value(r);
            if res == "W" {
                isWin = true;
            } else {
                isWin = false;
            }
            let mut items: Vec<String> = Vec::new();
            for i in (1..6) {
                items.push(mysql::from_value_opt(a.get(&*("equ_".to_owned() + &i.to_string())).ok_or(Error::from(core::fmt::Error))?)?);
            }
            let gameHistory = GameHistoryData {
                gameId: mysql::from_value_opt(a.get("game_id").ok_or(Error::from(core::fmt::Error))?)?,
                steamId: mysql::from_value_opt(a.get("steam_id").ok_or(Error::from(core::fmt::Error))?)?,
                hero: mysql::from_value_opt(a.get("hero").ok_or(Error::from(core::fmt::Error))?)?,
                mode: mysql::from_value_opt(a.get("mode").ok_or(Error::from(core::fmt::Error))?)?,
                level: mysql::from_value_opt(a.get("level").ok_or(Error::from(core::fmt::Error))?)?,
                isWin: isWin,
                k: mysql::from_value_opt(a.get("k").ok_or(Error::from(core::fmt::Error))?)?,
                d: mysql::from_value_opt(a.get("d").ok_or(Error::from(core::fmt::Error))?)?,
                a: mysql::from_value_opt(a.get("a").ok_or(Error::from(core::fmt::Error))?)?,
                cs: mysql::from_value_opt(a.get("killed_unit").ok_or(Error::from(core::fmt::Error))?)?,
                money: mysql::from_value_opt(a.get("income").ok_or(Error::from(core::fmt::Error))?)?,
                playTime: mysql::from_value_opt(a.get("play_time").ok_or(Error::from(core::fmt::Error))?)?,
                date: mysql::from_value_opt(a.get("createtime").ok_or(Error::from(core::fmt::Error))?)?,
                items: items,
            };
            gameHistorysData.push(gameHistory);
        } else {
            warn!("mysql error: {}, line  : {}", sql, line!());
        }
    }
    msgtx.try_send(MqttMsg {
        topic: format!("member/{}/res/get_game_historys", id),
        msg: serde_json::to_string(&gameHistorysData)?,
    })?;
    Ok(())
}

pub fn Binding(
    id: String,
    v: Value,
    pool: mysql::Pool,
    msgtx: Sender<MqttMsg>,
) -> std::result::Result<(), Error> {
    let data: BindingData = serde_json::from_value(v)?;
    let mut count = 0;
    let mut conn = pool.get_conn()?;
    let mut sql = format!(
        r#"select * from twitch where id="{}""#,
        data.id
    );
    let qres: mysql::QueryResult = conn.query(sql.clone())?;
    for row in qres {
        count += 1;
    }
    if count > 0 {
        msgtx.try_send(MqttMsg {
            topic: format!("member/{}/res/binding", id),
                msg: format!(r#"{{"msg":"fail"}}"#)}
        )?;
    } else {
        sql = format!(
            r#"insert into twitch (id, steam_id, token) values ('{}', '{}', '{}')"#,
            data.id, data.steam_id, data.token
        );
        conn.query(sql.clone())?;
        msgtx.try_send(MqttMsg {
            topic: format!("member/{}/res/binding", id),
                msg: format!(r#"{{"msg":"Ok"}}"#)}
        )?;
    }
    Ok(())
}

pub fn CheckBinding(
    id: String,
    v: Value,
    pool: mysql::Pool,
    msgtx: Sender<MqttMsg>,
) -> std::result::Result<(), Error> {
    let data: CheckBindingData = serde_json::from_value(v)?;
    let mut count = 0;
    let mut conn = pool.get_conn()?;
    let mut sql = format!(
        r#"select * from twitch where steam_id="{}""#,
        data.steam_id
    );
    let qres: mysql::QueryResult = conn.query(sql.clone())?;
    for row in qres {
        count += 1;
    }
    if count > 0 {
        msgtx.try_send(MqttMsg {
            topic: format!("member/{}/res/check_binding", id),
                msg: format!(r#"{{"msg":"Ok"}}"#)}
        )?;
    } else {
        msgtx.try_send(MqttMsg {
            topic: format!("member/{}/res/check_binding", id),
                msg: format!(r#"{{"msg":"fail"}}"#)}
        )?;
    }
    Ok(())
}

pub fn GetLeaderboard(
    id: String,
    v: Value,
    pool: mysql::Pool,
    msgtx: Sender<MqttMsg>,
) -> std::result::Result<(), Error> {
    let mut conn = pool.get_conn()?;
    let mut rkScores: Vec<ScoreData> = vec![];
    let mut atScores: Vec<ScoreData> = vec![];
    let mut rkSql = format!(
        r#"select id, rk, count(res) as win from user, Finished_detail where id = steam_id and mode = 'rk' and res = 'W' group by steam_id order by rk desc limit 30;"#
    );
    let qres: mysql::QueryResult = conn.query(rkSql.clone())?;
    for row in qres {
        let a = row?.clone();
        let data = ScoreData {
            steamID: mysql::from_value_opt(a.get("id").ok_or(Error::from(core::fmt::Error))?)?,
            score: mysql::from_value_opt(a.get("rk").ok_or(Error::from(core::fmt::Error))?)?,
            win: mysql::from_value_opt(a.get("win").ok_or(Error::from(core::fmt::Error))?)?,
        };
        rkScores.push(data);
    }
    let mut atSql = format!(
        r#"select id, at, count(res) as win from user, Finished_detail where id = steam_id and mode = 'at' and res = 'W' group by steam_id order by at desc limit 30;"#
    );
    let qres: mysql::QueryResult = conn.query(atSql.clone())?;
    for row in qres {
        let a = row?.clone();
        let data = ScoreData {
            steamID: mysql::from_value_opt(a.get("id").ok_or(Error::from(core::fmt::Error))?)?,
            score: mysql::from_value_opt(a.get("at").ok_or(Error::from(core::fmt::Error))?)?,
            win: mysql::from_value_opt(a.get("win").ok_or(Error::from(core::fmt::Error))?)?,
        };
        atScores.push(data);
    }
    let mut leaderBoard = LeaderboardData {
        rkLeaderboard: rkScores,
        atLeaderboard: atScores,
    };
    msgtx.try_send(MqttMsg {
        topic: format!("member/{}/res/get_leaderboard", id),
            msg: serde_json::to_string(&leaderBoard)?,
    })?;
    Ok(())
}