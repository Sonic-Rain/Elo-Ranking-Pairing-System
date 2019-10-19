use serde_json::{self, Result, Value};
use std::env;
use std::io::{self, Write};
use serde_derive::{Serialize, Deserialize};
use failure::Error;

use log::{info, warn, error, trace};

use ::futures::Future;
use mysql;
use crossbeam_channel::{bounded, tick, Sender, Receiver, select};
use crate::event_room::*;
use crate::room::User;

#[derive(Serialize, Deserialize)]
struct LoginData {
    id: String,
}

#[derive(Serialize, Deserialize)]
struct LogoutData {
    id: String,
}


pub fn login(id: String, v: Value, pool: mysql::Pool, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: LoginData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn()?;
    let sql = format!(r#"select a.score as ng, b.score as rk, name from user as c 
                        join user_ng as a on a.id=c.id 
                        join user_rank as b on b.id=c.id  where userid='{}';"#, data.id);
    let qres2: mysql::QueryResult = conn.query(sql.clone())?;
    let mut ng: i16 = 0;
    let mut rk: i16 = 0;
    let mut name: String = "".to_owned();
    
    let mut count = 0;
    for row in qres2 {
        count += 1;
        let a = row?.clone();
        ng = mysql::from_value(a.get("ng").unwrap());
        rk = mysql::from_value(a.get("rk").unwrap());
        name = mysql::from_value(a.get("name").unwrap());
        break;
    }
    if count == 0 {
        let sql = format!("insert into user (userid, name, status) values ('{}', 'default name', 'online');", data.id);
        {
            conn.query(sql.clone())?;
        }
        let sql = format!("select id from user where userid='{}';", data.id);
        println!("sql: {}", sql);
        let qres = conn.query(sql.clone())?;
        let mut id = -1;
        for row in qres {
            let a = row?.clone();
            id = mysql::from_value(a.get("id").unwrap());
        }
        if id > 0 {
            ng = 1000;
            rk = 1000;
            let sql = format!("insert into user_rank (id, score) values ({}, 1000);", id);
            conn.query(sql.clone())?;
            let sql = format!("insert into user_ng (id, score) values ({}, 1000);", id);
            conn.query(sql.clone())?;
        }
    }
    let qres = conn.query(format!("update user set status='online' where userid='{}';", data.id));
    let publish_packet = match qres {
        Ok(_) => {
            //sender.send(RoomEventData::Login(UserLoginData {u: User { id: id.clone(), ng: ng, rk: rk}}));
        },
        _=> {
      
        }
    };
    sender.send(RoomEventData::Login(UserLoginData {u: User { id: id.clone(), hero: name.clone(), online: true, ng: ng, rk: rk, ..Default::default()}}));
    Ok(())
}

pub fn logout(id: String, v: Value, pool: mysql::Pool, sender: Sender<RoomEventData>)
 -> std::result::Result<(), Error>
{
    let data: LogoutData = serde_json::from_value(v)?;
    let mut conn = pool.get_conn()?;
    let qres = conn.query(format!("update user set status='offline' where userid='{}';", data.id));
    let publish_packet = match qres {
        Ok(_) => {
            //sender.send(RoomEventData::Logout(UserLogoutData { id: id}));
        },
        _=> {
            
        }
    };
    sender.send(RoomEventData::Logout(UserLogoutData { id: id}));
    Ok(())
}

