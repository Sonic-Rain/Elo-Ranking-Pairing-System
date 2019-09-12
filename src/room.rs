use serde_derive::{Serialize, Deserialize};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone, Debug, PartialEq, Default)]
pub struct User {
    pub id: String,
    pub hero: String,
    pub ng: u16,
    pub rk: u16,
    pub rid: u32,
}

#[derive(Clone, Debug)]
pub struct RoomData {
    pub rid: u32,
    pub users: Vec<Rc<RefCell<User>>>,
    pub master: String,
    pub avg_ng: u16,
    pub avg_rk: u16,
    pub ready: i8,
}

impl RoomData {
    pub fn update_avg(&mut self) {
        let mut sum_ng = 0;
        let mut sum_rk = 0;
        for user in &self.users {
            sum_ng += user.borrow().ng;
            sum_rk += user.borrow().rk;
        }
        if self.users.len() > 0 {
            self.avg_ng = sum_ng/self.users.len() as u16;
            self.avg_rk = sum_rk/self.users.len() as u16;
        }
    }

    pub fn add_user(&mut self, user: Rc<RefCell<User>>) {
        user.borrow_mut().rid = self.rid;
        self.users.push(Rc::clone(&user));
        self.update_avg();
    }

    pub fn rm_user(&mut self, id: &String) {
        let mut i = 0;
        while i != self.users.len() {
            if self.users[i].borrow().id == *id {
                self.users[i].borrow_mut().rid = 0;
                self.users.remove(i);
            } else {
                i += 1;
            }
        }
        self.update_avg();
    }
}

#[derive(Clone, Debug, Default)]
pub struct FightCheck {
    id: String,
    check: i8,
}

#[derive(Clone, Debug, Default)]
pub struct FightGroup {
    pub rooms: Vec<Rc<RefCell<RoomData>>>,
    pub user_count: u16,
    pub avg_ng: u16,
    pub avg_rk: u16,
    pub checks: Vec<FightCheck>,
    pub rids: Vec<u32>,
    pub game_status: u16,
}

impl FightGroup {
    pub fn user_ready(&mut self, id: &String) -> bool {
        for c in &mut self.checks {
            if c.id == *id {
                c.check = 1;
                return true;
            }
        }
        false
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

    pub fn check_has_room(&self, id: &String) -> bool {
        for room in &self.rooms {
            if room.borrow().master == *id {
                return true;
            }
        }
        false
    }

    pub fn update_avg(&mut self) {
        let mut sum_ng: u32 = 0;
        let mut sum_rk: u32 = 0;
        self.user_count = 0;
        for room in &self.rooms {
            sum_ng += room.borrow().avg_ng as u32;
            sum_rk += room.borrow().avg_rk as u32;
            self.user_count += room.borrow().users.len() as u16;
        }
        if self.user_count > 0 {
            self.avg_ng = (sum_ng/self.user_count as u32) as u16;
            self.avg_rk = (sum_rk/self.user_count as u32) as u16;
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
    
    pub fn prestart(&mut self) {
        self.checks.clear();
        for room in &self.rooms {
            room.borrow_mut().ready = 1;
            for u in &room.borrow().users {
                self.checks.push(FightCheck{id: u.borrow().id.clone(), check: 0});
            }
        }
    }

    pub fn check_prestart(&self) -> PrestartStatus {
        let mut res = PrestartStatus::Ready;
        for c in &self.checks {
            if c.check < 0 {
                return PrestartStatus::Cancel;
            } else if c.check !=1 {
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
    pub user_count: u16,
    pub winteam: i16,
    pub game_status: u16,
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
        for t in &self.teams {
            for r in &t.borrow().rooms {
                self.room_names.push(r.borrow().master.clone());
            }
        }
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

    pub fn ready(&mut self) {
        for g in &mut self.teams {
            g.borrow_mut().ready();
        }
    }
}