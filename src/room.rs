use crate::msg::*;
use log::{error, info, trace, warn};
use crossbeam_channel::{bounded, select, tick, Receiver, Sender};
use failure::Error;
use serde_derive::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone, Debug, PartialEq, Default)]
pub struct User {
    pub id: String,
    pub name: String,
    pub hero: String,
    pub ban_hero: String,
    pub ng: i16,
    pub rk: i16,
    pub at: i16,
    pub rid: u64,
    pub gid: u64,
    pub game_id: u64,
    pub online: bool,
    pub start_prestart: bool,
    pub start_get: bool,
    pub raindrop: u64,
    pub first_win: bool,
    pub isLocked: bool,
    pub isLoading: bool,
}

#[derive(Clone, Debug)]
pub struct RoomData {
    pub rid: u64,
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
    pub fn check_lock(&mut self) -> bool{
        let mut res = true;
        // println!("self users : {:?}", self.users);
        for u in &self.users {
            if !u.borrow().isLocked {
                res = false;
            }
        }
        res
    }

    pub fn check_loading(&mut self) -> bool{
        let mut res = true;
        println!("self users : {:?}", self.users);
        for u in &self.users {
            if !u.borrow().isLoading {
                res = false;
            }
        }
        res
    }
    pub fn update_avg(&mut self) {
        let mut highest_ng = 0;
        let mut highest_rk = 0;
        let mut highest_at = 0;
        for user in &self.users {
            if user.borrow().ng > highest_ng {
                highest_ng = user.borrow().ng;
            }
            if user.borrow().rk > highest_rk {
                highest_rk = user.borrow().rk;
            }
            if user.borrow().rk > highest_rk {
                highest_rk = user.borrow().rk;
            }
        }
        self.avg_ng = highest_ng + 20 * self.users.len() as i16;
        self.avg_rk = highest_rk + 20 * self.users.len() as i16;
        self.avg_at = highest_at + 20 * self.users.len() as i16;
        // let mut sum_ng = 0;
        // let mut sum_rk = 0;
        // let mut sum_at = 0;
        // for user in &self.users {
        //     sum_ng += user.borrow().ng;
        //     sum_rk += user.borrow().rk;
        //     sum_at += user.borrow().at;
        // }
        // if self.users.len() > 0 {
        //     self.avg_ng = sum_ng / self.users.len() as i16;
        //     self.avg_rk = sum_rk / self.users.len() as i16;
        //     self.avg_at = sum_at / self.users.len() as i16;
        // }
        // if self.users.len() > 1 {
        //     self.avg_ng += 10 * self.users.len() as i16;
        //     self.avg_rk += 10 * self.users.len() as i16;
        //     self.avg_at += 10 * self.users.len() as i16;
        // }
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

    pub fn check_start_get(&mut self) -> bool {
        let mut res = false;
        for user in &self.users {
            // println!("User: {}, Prestart_get: {}", user.borrow().id, user.borrow().start_get);
            if user.borrow().start_get == true {
                res = true;
            } else {
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

    pub fn publish_update(&self, msgtx: &Sender<MqttMsg>, r: String) -> Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        pub struct teamCell {
            pub room: String,
            pub team: Vec<String>,
        }
        let mut t = teamCell {
            room: self.master.clone(),
            team: vec![],
        };
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
        msgtx.try_send(MqttMsg {
            topic: format!("room/{}/res/update", r),
            msg: serde_json::to_string(&t)?,
        })?;
        Ok(())
    }

    pub fn rm_user(&mut self, id: &String) {
        let mut i = 0;
        while i != self.users.len() {
            let id2 = self.users[i].borrow().id.clone();
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
    pub rids: Vec<u64>,
    pub game_status: u16,
    pub queue_cnt: i16,
}

impl FightGroup {

    pub fn check_lock(&mut self) -> bool{
        let mut res = true;
        for room in &self.rooms {
            if !room.borrow_mut().check_lock() {
                res = false;
            }
        }
        res
    }

    pub fn check_loading(&mut self) -> bool {
        let mut res = true;
        for room in &self.rooms {
            if !room.borrow_mut().check_loading() {
                res = false;
            }
        }
        res
    }

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
                res.push((
                    u.borrow().id.clone(),
                    u.borrow().name.clone(),
                    u.borrow().hero.clone(),
                ));
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
            self.avg_ng = (sum_ng / self.user_count as i32) as i16;
            self.avg_rk = (sum_rk / self.user_count as i32) as i16;
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

    pub fn rm_room_by_rid(&mut self, id: u64) {
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

    pub fn set_group_id(&mut self, gid: u64) {
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
                self.checks.push(FightCheck {
                    id: u.borrow().id.clone(),
                    check: 0,
                });
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
                res = PrestartStatus::Wait;
            }
        }
        res
    }
}

#[derive(PartialEq)]
pub enum FightGameStatus {
    Ready,
    Loading,
    Ban,
    Pick,
    ReadyToStart,
    Gaming,
    Finished,
}

#[derive(PartialEq)]
pub enum RkPickStatus {
    Round0,
    Round1,
    Round2,
    Round3,
    Round4,
    Round5,
    Round6,
    Round7,
}

#[derive(Clone, Debug, Default)]
pub struct FightGame {
    pub teams: Vec<Rc<RefCell<FightGroup>>>,
    pub room_names: Vec<String>,
    pub user_names: Vec<String>,
    pub pick_position: Vec<usize>,
    pub game_id: u64,
    pub user_count: u16,
    pub winteam: i16,
    pub game_status: u16,
    pub game_port: u16,
    pub ready_cnt: f32,
    pub loading_cnt: u16,
    pub ban_time: i16,
    pub choose_time: i16,
    pub ready_to_start_time: i16,
    pub mode: String,
    pub pick_status: u16,
    pub lock_cnt: u16,
    pub time: u64,
}

#[derive(PartialEq)]
pub enum PrestartStatus {
    Wait,
    Ready,
    Cancel,
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

    pub fn set_game_id(&mut self, gid: u64) {
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

    pub fn check_at_status(&mut self) -> FightGameStatus {
        let mut res = FightGameStatus::Ready;
        if self.game_status == 0 {
            res = FightGameStatus::Ready;
        }
        if self.game_status == 1 {
            res = FightGameStatus::Loading;
        }
        if self.game_status == 2 {
            res = FightGameStatus::Ban;
        }
        if self.game_status == 3 {
            res = FightGameStatus::Ban;
        }
        if self.game_status == 4 {
            res = FightGameStatus::Ban;
        }
        if self.game_status == 5 {
            res = FightGameStatus::Ban;
        }
        if self.game_status == 6 {
            res = FightGameStatus::Ban;
        }
        if self.game_status == 7 {
            res = FightGameStatus::Ban;
        }
        if self.game_status == 8 {
            res = FightGameStatus::Pick;
        }
        if self.game_status == 9 {
            res = FightGameStatus::Pick;
        }
        if self.game_status == 10 {
            res = FightGameStatus::Pick;
        }
        if self.game_status == 11 {
            res = FightGameStatus::Pick;
        }
        if self.game_status == 12 {
            res = FightGameStatus::Pick;
        }
        if self.game_status == 13 {
            res = FightGameStatus::Pick;
        }
        if self.game_status == 14 {
            res = FightGameStatus::Ban;
        }
        if self.game_status == 15 {
            res = FightGameStatus::Ban;
        }
        if self.game_status == 16 {
            res = FightGameStatus::Ban;
        }
        if self.game_status == 17 {
            res = FightGameStatus::Ban;
        }        
        if self.game_status == 18 {
            res = FightGameStatus::Pick;
        }
        if self.game_status == 19 {
            res = FightGameStatus::Pick;
        }
        if self.game_status == 20 {
            res = FightGameStatus::Pick;
        }
        if self.game_status == 21 {
            res = FightGameStatus::Pick;
        }
        if self.game_status == 22 {
            res = FightGameStatus::ReadyToStart;
        }
        if self.game_status == 23 {
            res = FightGameStatus::Gaming;
        }
        if self.game_status == 24 {
            res = FightGameStatus::Finished;
        }
        res
    }

    pub fn check_status(&mut self) -> FightGameStatus {
        let mut res = FightGameStatus::Ready;
        if self.game_status == 0 {
            res = FightGameStatus::Ready;
        } else if self.game_status == 1 {
            res = FightGameStatus::Loading;
        } else if self.game_status == 2 {
            res = FightGameStatus::Ban;
        } else if self.game_status == 3 {
            res = FightGameStatus::Pick;
        } else if self.game_status == 4 {
            res = FightGameStatus::ReadyToStart;
        } else if self.game_status == 5 {
            res = FightGameStatus::Gaming;
        } else if self.game_status == 6 {
            res = FightGameStatus::Finished;
        }
        res
    }

    pub fn check_rk_pick_status(&mut self) -> RkPickStatus {
        let mut res = RkPickStatus::Round0;
        if self.pick_status == 0 {
            res = RkPickStatus::Round0;
        } else if self.pick_status == 1 {
            res = RkPickStatus::Round1;
        } else if self.pick_status == 2 {
            res = RkPickStatus::Round2;
        } else if self.pick_status == 3 {
            res = RkPickStatus::Round3;
        } else if self.pick_status == 4 {
            res = RkPickStatus::Round4;
        } else if self.pick_status == 5 {
            res = RkPickStatus::Round5;
        } else if self.pick_status == 6 {
            res = RkPickStatus::Round6;
        } else if self.pick_status == 7 {
            res = RkPickStatus::Round7;
        }
        res
    }

    pub fn next_status(&mut self) {
        self.game_status += 1;
        info!("game_id : {}, status: {}, line: {}", self.game_id, self.game_status, line!());
        if self.game_status == 2 {
            self.pick_position = vec![0];
        }
        if self.game_status == 3 {
            self.pick_position = vec![5];
        }
        if self.game_status == 4 {
            self.pick_position = vec![1];
        }
        if self.game_status == 5 {
            self.pick_position = vec![6];
        }
        if self.game_status == 6 {
            self.pick_position = vec![2];
        }
        if self.game_status == 7 {
            self.pick_position = vec![7];
        }
        if self.game_status == 8 {
            self.pick_position = vec![0];
        }
        if self.game_status == 9 {
            self.pick_position = vec![5];
        }
        if self.game_status == 10 {
            self.pick_position = vec![6];
        }
        if self.game_status == 11 {
            self.pick_position = vec![1];
        }
        if self.game_status == 12 {
            self.pick_position = vec![2];
        }
        if self.game_status == 13 {
            self.pick_position = vec![7];
        }
        if self.game_status == 14 {
            self.pick_position = vec![8];
        }
        if self.game_status == 15 {
            self.pick_position = vec![3];
        }
        if self.game_status == 16 {
            self.pick_position = vec![9];
        }
        if self.game_status == 17 {
            self.pick_position = vec![4];
        }        
        if self.game_status == 18 {
            self.pick_position = vec![8];
        }
        if self.game_status == 19 {
            self.pick_position = vec![3];
        }
        if self.game_status == 20 {
            self.pick_position = vec![4];
        }
        if self.game_status == 21 {
            self.pick_position = vec![9];
        }
    }

    pub fn set_mode(&mut self, mode: String) {
        self.mode = mode;
    }

    pub fn next_pick_status(&mut self) {
        self.lock_cnt = 0;
        self.pick_status += 1;
        if self.mode == "ng" {
            self.pick_position = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        }
        if self.mode == "rk" {
            if self.check_rk_pick_status() == RkPickStatus::Round1 {
                self.pick_position = vec![0];
            } else if self.check_rk_pick_status() == RkPickStatus::Round2 {
                self.pick_position = vec![5, 6];
            } else if self.check_rk_pick_status() == RkPickStatus::Round3 {
                self.pick_position = vec![1, 2];
            } else if self.check_rk_pick_status() == RkPickStatus::Round4 {
                self.pick_position = vec![7, 8];
            } else if self.check_rk_pick_status() == RkPickStatus::Round5 {
                self.pick_position = vec![3, 4];
            } else if self.check_rk_pick_status() == RkPickStatus::Round6 {
                self.pick_position = vec![9];
            } else if self.check_rk_pick_status() == RkPickStatus::Round7 {
                self.pick_position = vec![];
            }
        }
    }
}
