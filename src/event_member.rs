

use mqtt;
use mqtt::packet::*;
use serde_json::{self, Result, Value};
use mqtt::{Decodable, Encodable, QualityOfService};
use mqtt::{TopicFilter, TopicName};
use std::env;
use std::io::{self, Write};
use serde_derive::{Serialize, Deserialize};
use std::io::{Error, ErrorKind};

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


pub fn login(stream: &mut std::net::TcpStream, id: String, v: Value, pool: mysql::Pool, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: LoginData = serde_json::from_value(v).unwrap();
    let mut conn = pool.get_conn().unwrap();
    let qres2 = conn.query(format!("select ng, rk from user as a join user_rank as b on a.id=b.id where userid='{}';", data.id)).unwrap();
    let mut ng: u16 = 0;
    let mut rk: u16 = 0;
    for row in qres2 {
        let a = row.unwrap().clone();
        ng = mysql::from_value(a.get("ng").unwrap());
        rk = mysql::from_value(a.get("rk").unwrap());
        break;
    }

    let qres = conn.query(format!("update user set status='online' where userid='{}';", data.id));
    let publish_packet = match qres {
        Ok(_) => {
      
            //sender.send(RoomEventData::Login(UserLoginData {u: User { id: id.clone(), ng: ng, rk: rk}}));
        },
        _=> {
      
        }
    };
    sender.send(RoomEventData::Login(UserLoginData {u: User { id: id.clone(), ng: ng, rk: rk}}));
    Ok(())
}

pub fn logout(stream: &mut std::net::TcpStream, id: String, v: Value, pool: mysql::Pool, sender: Sender<RoomEventData>)
 -> std::result::Result<(), std::io::Error>
{
    let data: LogoutData = serde_json::from_value(v).unwrap();
    let mut conn = pool.get_conn().unwrap();
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
