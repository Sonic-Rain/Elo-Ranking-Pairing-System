use serde_derive::{Serialize, Deserialize};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct User {
    pub id: String,
    pub ng: u16,
    pub rk: u16,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RoomData {
    pub rid: u32,
    pub users: Vec<User>,
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
            sum_ng += user.ng;
            sum_rk += user.rk;
        }
        if self.users.len() > 0 {
            self.avg_ng = sum_ng/self.users.len() as u16;
            self.avg_rk = sum_rk/self.users.len() as u16;
        }
    }

    pub fn add_user(&mut self, user: &User) {
        self.users.push(user.clone());
        self.update_avg();
    }

    pub fn rm_user(&mut self, id: &String) {
        let mut i = 0;
        while i != self.users.len() {
            if self.users[i].id == *id {
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
    check: u8,
}

#[derive(Clone, Debug, Default)]
pub struct FightGroup {
    pub rooms: Vec<Rc<RefCell<RoomData>>>,
    pub user_count: u16,
    pub avg_ng: u16,
    pub avg_rk: u16,
    pub checks: Vec<FightCheck>,
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
        self.update_avg();
    }

    pub fn rm_room(&mut self, id: &String) {
        let mut i = 0;
        while i != self.rooms.len() {
            if self.rooms[i].borrow().master == *id {
                self.rooms.remove(i);
            } else {
                i += 1;
            }
        }
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
                self.checks.push(FightCheck{id: u.id.clone(), check: 0});
            }
        }
    }

    pub fn check_prestart(&self) -> bool {
        for c in &self.checks {
            if c.check != 1 {
                return false;
            }
        }
        true
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

impl FightGame {
    pub fn update_names(&mut self) {
        self.room_names.clear();
        for t in &self.teams {
            for r in &t.borrow().rooms {
                self.room_names.push(r.borrow().master.clone());
            }
        }
    }

    pub fn check_prestart(&self) -> bool {
        for c in &self.teams {
            if c.borrow().check_prestart() == false {
                return false;
            }
        }
        true
    }

    pub fn ready(&mut self) {
        for g in &mut self.teams {
            g.borrow_mut().ready();
        }
    }
}