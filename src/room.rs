use serde_derive::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct User {
    pub id: String,
    pub ng: i16,
    pub rk: i16,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RoomData {
    pub rid: i32,
    pub users: Vec<User>,
    pub avg_ng: i16,
    pub avg_rk: i16,
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
            self.avg_ng = sum_ng/self.users.len() as i16;
            self.avg_rk = sum_rk/self.users.len() as i16;
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

