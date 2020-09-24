use serde_derive::{Serialize, Deserialize};
use std::cell::RefCell;
use std::rc::Rc;
use crate::msg::*;
use crossbeam_channel::{bounded, tick, Sender, Receiver, select};
use failure::Error;

#[derive(Clone, Debug, PartialEq, Default)]
pub struct User {
    pub id: String,
    pub name: String,
    pub hero: String,
    pub ng: i16,
    pub rk: i16,
    pub at: i16,
    pub rid: u32,
    pub gid: u32,
    pub game_id: u32,
    pub online: bool,
    pub start_prestart: bool,
    pub start_get: bool,
}

#[derive(Clone, Debug)]
pub struct RoomData {
    pub rid: u32,
    pub users: Vec<Rc<RefCell<User>>>,
    pub master: String,
    pub last_master: String,
    pub avg_ng: i16,
    pub avg_rk: i16,
    pub avg_at: i16,
    pub ready: i8,
    pub queue_cnt: i16,
    pub mode: String,
}

impl RoomData {
    pub fn update_avg(&mut self) {
        let mut sum_ng = 0;
        let mut sum_rk = 0;
        let mut sum_at = 0;
        for user in &self.users {
            sum_ng += user.borrow().ng;
            sum_rk += user.borrow().rk;
            sum_at += user.borrow().at;
        }
        if self.users.len() > 0 {
            self.avg_ng = sum_ng/self.users.len() as i16;
            self.avg_rk = sum_rk/self.users.len() as i16;
            self.avg_at = sum_at/self.users.len() as i16;
        }
    }

    pub fn add_user(&mut self, user: Rc<RefCell<User>>) {
        let mut isInTeam = false;
        for u in &self.users {
            if user.borrow().id == u.borrow().id {
                isInTeam = true;
            }
        }
        if !isInTeam {
            user.borrow_mut().rid = self.rid;
            self.users.push(Rc::clone(&user));
            self.update_avg();
        }
    }

    pub fn leave_room(&mut self) {
        for user in &self.users {
            user.borrow_mut().rid = 0;
            user.borrow_mut().gid = 0;
            user.borrow_mut().game_id = 0;
        }
        self.ready = 0;
    }

    pub fn user_prestart(&mut self) {
        for user in &self.users {
            user.borrow_mut().start_prestart = true;
            user.borrow_mut().start_get = false;
        }
    }

    pub fn check_start_get(&mut self) -> bool{
        let mut res = false;
        for user in &self.users {
            //println!("User: {}, Prestart_get: {}", user.borrow().id, user.borrow().prestart_get);
            if user.borrow().start_get == true {
                res = true;
            }
            else {
                res = false;
                break;
            }
        }
        res
    }

    pub fn clear_queue(&mut self) {
        for user in &self.users {
            user.borrow_mut().gid = 0;
            user.borrow_mut().game_id = 0;
        }
        self.ready = 0;
    }

    pub fn publish_update(&self, msgtx: &Sender<MqttMsg>, r: String) -> Result<(), Error>{
        #[derive(Serialize, Deserialize)]
        pub struct teamCell {
            pub room: String,  
            pub team: Vec<String>,
        }
        let mut t = teamCell {room: self.master.clone(), team: vec![]};
        
        for user in &self.users {
            let mut isInTeam = false;
            for u in &t.team {
                if user.borrow().id == u.clone() {
                    isInTeam = true;
                    break;
                }
            }
            if !isInTeam {
                t.team.push(user.borrow().id.clone());
            }
        }
        msgtx.try_send(MqttMsg{topic:format!("room/{}/res/update", r), msg: serde_json::to_string(&t).unwrap()})?;
        Ok(())
    }

    pub fn rm_user(&mut self, id: &String) {
        let mut i = 0;
        while i != self.users.len() {
            let id2 =  self.users[i].borrow().id.clone();
            if id2 == *id {
                self.users.remove(i);
            } else {
                i += 1;
            }
        }
        if self.master == *id && self.users.len() > 0 {
            self.last_master = self.master.clone();
            self.master = self.users[0].borrow().id.clone();
        }
        self.update_avg();
    }
}

#[derive(Clone, Debug, Default)]
pub struct FightCheck {
    pub id: String,
    pub check: i8,
}

#[derive(Clone, Debug, Default)]
pub struct FightGroup {
    pub rooms: Vec<Rc<RefCell<RoomData>>>,
    pub user_count: i16,
    pub avg_ng: i16,
    pub avg_rk: i16,
    pub checks: Vec<FightCheck>,
    pub rids: Vec<u32>,
    pub game_status: u16,
    pub queue_cnt: i16,
}

impl FightGroup {
    pub fn user_ready(&mut self, id: &String) -> bool {
        for c in &mut self.checks {
            //println!("c: {}, id: {}", c.id, *id);
            if c.id == *id {
                //println!("check");
                c.check = 1;
                self.check_prestart();
                return true;
            }
        }
        false
    }

    pub fn get_users_id_hero(&self) -> Vec<(String, String, String)> {
        let mut res: Vec<(String, String, String)> = vec![];
        for r in &self.rooms {
            for u in &r.borrow().users {
                res.push((u.borrow().id.clone(), u.borrow().name.clone(), u.borrow().hero.clone()));
            }
        }
        res
    }
    pub fn user_cancel(&mut self, id: &String) -> bool {
        for c in &mut self.checks {
            if c.id == *id {
                c.check = -1;
                return true;
            }
        }
        false
    }
    pub fn leave_room(&mut self) -> bool {
        for r in &mut self.rooms {
            r.borrow_mut().leave_room();
        }
        false
    }
    pub fn clear_queue(&mut self) -> bool {
        for r in &mut self.rooms {
            r.borrow_mut().clear_queue();
        }
        false
    }

    pub fn check_has_room(&self, id: &String) -> bool {
        for room in &self.rooms {
            if room.borrow().master == *id {
                return true;
            }
        }
        false
    }

    pub fn update_avg(&mut self) {
        let mut sum_ng: i32 = 0;
        let mut sum_rk: i32 = 0;
        self.user_count = 0;
        for room in &self.rooms {
            sum_ng += room.borrow().avg_ng as i32 * room.borrow().users.len() as i32;
            sum_rk += room.borrow().avg_rk as i32 * room.borrow().users.len() as i32;
            self.user_count += room.borrow().users.len() as i16;
        }
        if self.user_count > 0 {
            self.avg_ng = (sum_ng/self.user_count as i32) as i16;
            self.avg_rk = (sum_rk/self.user_count as i32) as i16;
        }
    }

    pub fn add_room(&mut self, room: Rc<RefCell<RoomData>>) {
        self.rooms.push(Rc::clone(&room));
        self.rids.push(room.borrow().rid);
        self.update_avg();
    }

    pub fn rm_room_by_master(&mut self, id: &String) {
        let pos = self.rooms.iter().position(|x| x.borrow().master == *id);
        if let Some(x) = pos {
            let rid = self.rooms[x].borrow().rid;
            self.rids.retain(|&x| x != rid);
            self.rooms.remove(x);
        }
        self.update_avg();
    }

    pub fn rm_room_by_rid(&mut self, id: u32) {
        let mut i = 0;
        self.rids.retain(|&x| x != id);
        self.rooms.retain(|x| x.borrow().rid != id);
        self.update_avg();
    }

    pub fn ready(&mut self) {
        self.checks.clear();
        for room in &self.rooms {
            room.borrow_mut().ready = 3;
        }
    }

    pub fn set_group_id(&mut self, gid: u32) {
        for room in &self.rooms {
            room.borrow_mut().ready = 1;
            for u in &room.borrow_mut().users {
                u.borrow_mut().gid = gid;
            }
        }
    }
    
    pub fn prestart(&mut self) {
        self.checks.clear();
        for room in &self.rooms {
            room.borrow_mut().ready = 1;
            room.borrow_mut().user_prestart();
            for u in &room.borrow().users {
                self.checks.push(FightCheck{id: u.borrow().id.clone(), check: 0});
            }
        }
    }

    pub fn check_prestart(&self) -> PrestartStatus {
        let mut res = PrestartStatus::Ready;
        for c in &self.checks {
            // println!("check: '{:?}'", c);
            if c.check < 0 {
                return PrestartStatus::Cancel;
            } else if c.check != 1 {
                res= PrestartStatus::Wait;
            }
        }
        res
    }
}


#[derive(Clone, Debug, Default)]
pub struct FightGame {
    pub teams: Vec<Rc<RefCell<FightGroup>>>,
    pub room_names: Vec<String>,
    pub user_names: Vec<String>,
    pub game_id: u32,
    pub user_count: u16,
    pub winteam: i16,
    pub game_status: u16,
    pub game_port: u16,
    pub ready_cnt: u16,
    pub choose_time: u16,
}

#[derive(PartialEq)]
pub enum PrestartStatus {
    Wait,
    Ready,
    Cancel
}

impl FightGame {
    pub fn update_names(&mut self) {
        self.room_names.clear();
        self.user_names.clear();
        for t in &self.teams {
            for r in &t.borrow().rooms {
                self.room_names.push(r.borrow().master.clone());
                for u in &r.borrow().users {
                    self.user_names.push(u.borrow().id.clone());
                }
            }
        }
    }

    pub fn check_start_get(&self) -> bool {
        let mut res = false;
        for c in &self.teams {
            for r in &c.borrow().rooms {
                res = r.borrow_mut().check_start_get();
                if res == false {
                    break;
                }
            }
            if res == false {
                break;
            }
        }
        res
    }

    pub fn check_prestart(&self) -> PrestartStatus {
        let mut res = PrestartStatus::Ready;
        for c in &self.teams {
            let v = c.borrow().check_prestart();
            if v == PrestartStatus::Cancel {
                return PrestartStatus::Cancel;
            } else if v == PrestartStatus::Wait {
                res = PrestartStatus::Wait;
            }
        }
        res
    }

    pub fn set_game_id(&mut self, gid: u32) {
        for t in &mut self.teams {
            for room in &mut t.borrow_mut().rooms {
                room.borrow_mut().ready = 1;
                for u in &room.borrow_mut().users {
                    u.borrow_mut().game_id = gid;
                }
            }
        }
        self.game_id = gid;
    }
    pub fn leave_room(&mut self) -> bool {
        for r in &mut self.teams {
            r.borrow_mut().leave_room();
        }
        false
    }
    pub fn clear_queue(&mut self) -> bool {
        for r in &mut self.teams {
            r.borrow_mut().clear_queue();
        }
        false
    }

    pub fn ready(&mut self) {
        for g in &mut self.teams {
            g.borrow_mut().ready();
        }
    }
}