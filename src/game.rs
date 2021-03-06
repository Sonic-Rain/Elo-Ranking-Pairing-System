use crate::msg::*;
use crate::room::*;
use crate::event_room::*;
use crossbeam_channel::{bounded, select, tick, Receiver, Sender};
use failure::Error;
use log::{error, info, trace, warn};
use serde_derive::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;
use rand::{seq::IteratorRandom, thread_rng};
use std::collections::{BTreeMap, HashMap};

pub const BUFFER: i16 = -5;
pub const CHOOSE_HERO_TIME: i16 = 30;
pub const NG_CHOOSE_HERO_TIME: i16 = 90;
pub const BAN_HERO_TIME: i16 = 25;
pub const READY_TO_START_TIME: i16 = 10;
pub const ARAM_READY_TO_START_TIME: i16 = 40;

#[derive(Clone, Debug, Default)]
pub struct NGGame {
    pub teams: Vec<Rc<RefCell<FightGroup>>>,
    pub room_names: Vec<String>,
    pub user_names: Vec<String>,
    pub pick_position: Vec<usize>,
    pub game_id: u64,
    pub user_count: u16,
    pub winteam: i16,
    pub game_status: u16,
    pub ban_time: i16,
    pub choose_time: i16,
    pub ready_to_start_time: i16,
    pub pick_status: u16,
    pub time: u64,
    pub ban_heros: Vec<Vec<String>>,
}

#[derive(Clone, Debug)]
pub enum NGGameStatus {
    Loading,
    Ban,
    Pick,
    ReadyToStart,
    Gaming,
    Finished,
}

impl NGGame {
    pub fn check_status(&mut self) -> NGGameStatus {
        let mut res = NGGameStatus::Loading;
        if self.game_status == 0 {
            res = NGGameStatus::Loading;
        }
        if self.game_status == 1 {
            res = NGGameStatus::Ban;
        }
        if self.game_status == 2 {
            res = NGGameStatus::Pick;
        }
        if self.game_status == 3 {
            res = NGGameStatus::ReadyToStart;
        }
        if self.game_status == 4 {
            res = NGGameStatus::Gaming;
        }
        if self.game_status == 5 {
            res = NGGameStatus::Finished;
        }
        res
    }
    pub fn next_status(&mut self) {
        let mut res = NGGameStatus::Loading;
        self.game_status += 1;
        info!(
            "NG game_id : {}, status: {}, status_code: {}, line: {}",
            self.game_id.clone(),
            self.get_status_name(),
            self.game_status,
            line!()
        );
        if self.game_status == 0 {}
        if self.game_status == 1 {
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        }
        if self.game_status == 2 {
            self.choose_time = NG_CHOOSE_HERO_TIME;
            self.pick_position = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        }
        if self.game_status == 3 {
            self.ready_to_start_time = READY_TO_START_TIME;
            self.pick_position = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        }
        if self.game_status == 4 {}
        if self.game_status == 5 {}
    }
    pub fn get_status_name(&mut self) -> String {
        let mut res = "loading";
        if self.game_status == 0 {
            res = "loading"
        }
        if self.game_status == 1 {
            res = "ban"
        }
        if self.game_status == 2 {
            res = "pick"
        }
        if self.game_status == 3 {
            res = "readyToStart";
        }
        if self.game_status == 4 {
            res = "gaming";
        }
        if self.game_status == 5 {
            res = "finished";
        }
        res.to_string()
    }
    pub fn check_loading(&mut self) -> bool {
        let mut res = true;
        for team in &self.teams {
            if !team.borrow_mut().check_loading() {
                res = false;
            }
        }
        res
    }
    pub fn check_lock(&mut self) -> bool {
        let mut res = true;
        for team in &self.teams {
            if !team.borrow_mut().check_lock() {
                res = false;
            }
        }
        res
    }
    pub fn rollBanHero(&mut self) {
        let mut result: Vec<Vec<String>> = Vec::new();
        for team in &self.teams {
            let mut heros: Vec<String> = Vec::new();
            for hero in team.borrow_mut().get_users_ban_hero() {
                if hero != "" {
                    heros.push(hero);
                }
            }
            let mut done: bool = false;
            let mut team_ban: Vec<String> = Vec::new();
            while !done {
                let mut rng = thread_rng();
                let mut hero: String = "".to_string();
                match heros.iter().choose(&mut rng) {
                    None => println!("no hero, line: {}", line!()),
                    Some(h) => {
                        team_ban.push(h.to_string());
                        hero = h.to_string();
                    },
                }
                heros.retain(|h| *h != hero);
                if team_ban.len() == 2 || heros.len() == 0{
                    done = true;
                }
            }
            result.push(team_ban);
        }
        self.ban_heros = result;
    }
}

#[derive(Clone, Debug, Default)]
pub struct RKGame {
    pub teams: Vec<Rc<RefCell<FightGroup>>>,
    pub room_names: Vec<String>,
    pub user_names: Vec<String>,
    pub pick_position: Vec<usize>,
    pub game_id: u64,
    pub user_count: u16,
    pub winteam: i16,
    pub game_status: u16,
    pub choose_time: i16,
    pub ban_time: i16,
    pub ready_to_start_time: i16,
    pub pick_status: u16,
    pub time: u64,
    pub ban_heros: Vec<Vec<String>>,
}

#[derive(Clone, Debug)]
pub enum RKGameStatus {
    Loading,
    Ban,
    Pick,
    ReadyToStart,
    Gaming,
    Finished,
}

impl RKGame {
    pub fn check_status(&mut self) -> RKGameStatus {
        let mut res = RKGameStatus::Loading;
        if self.game_status == 0 {
            res = RKGameStatus::Loading;
        }
        if self.game_status == 1 {
            res = RKGameStatus::Ban;
        }
        if self.game_status == 2 {
            res = RKGameStatus::Pick;
        }
        if self.game_status == 3 {
            res = RKGameStatus::Pick;
        }
        if self.game_status == 4 {
            res = RKGameStatus::Pick;
        }
        if self.game_status == 5 {
            res = RKGameStatus::Pick;
        }
        if self.game_status == 6 {
            res = RKGameStatus::Pick;
        }
        if self.game_status == 7 {
            res = RKGameStatus::Pick;
        }
        if self.game_status == 8 {
            res = RKGameStatus::ReadyToStart;
        }
        if self.game_status == 9 {
            res = RKGameStatus::Gaming;
        }
        if self.game_status == 10 {
            res = RKGameStatus::Finished;
        }
        res
    }

    pub fn next_status(&mut self) {
        self.game_status += 1;
        info!(
            "RK game_id : {}, status: {}, line: {}",
            self.game_id,
            self.game_status,
            line!()
        );
        if self.game_status == 0 {}
        if self.game_status == 1 {
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        }
        if self.game_status == 2 {
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![0];
        }
        if self.game_status == 3 {
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![5, 6];
        }
        if self.game_status == 4 {
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![1, 2];
        }
        if self.game_status == 5 {
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![7, 8];
        }
        if self.game_status == 6 {
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![3, 4];
        }
        if self.game_status == 7 {
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![9];
        }
        if self.game_status == 8 {
            self.ready_to_start_time = READY_TO_START_TIME;
            self.pick_position = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        }
        if self.game_status == 9 {}
        if self.game_status == 10 {}
    }
    pub fn check_loading(&mut self) -> bool {
        let mut res = true;
        for team in &self.teams {
            if !team.borrow_mut().check_loading() {
                res = false;
            }
        }
        res
    }
    pub fn check_lock(&mut self) -> bool {
        let mut res = true;
        for team in &self.teams {
            if !team.borrow_mut().check_lock() {
                res = false;
            }
        }
        res
    }
    pub fn updateBanHeros(&mut self) {
        let mut result: Vec<Vec<String>> = Vec::new();
        for team in &self.teams {
            let mut heros: Vec<String> = Vec::new();
            for hero in team.borrow_mut().get_users_ban_hero() {
                println!("{}", hero);
                heros.push(hero);
            }
            result.push(heros);
        }
        self.ban_heros = result;
    }
}

#[derive(Clone, Debug, Default)]
pub struct ATGame {
    pub teams: Vec<Rc<RefCell<FightGroup>>>,
    pub room_names: Vec<String>,
    pub user_names: Vec<String>,
    pub pick_position: Vec<usize>,
    pub game_id: u64,
    pub user_count: u16,
    pub winteam: i16,
    pub game_status: u16,
    pub choose_time: i16,
    pub ban_time: i16,
    pub ready_to_start_time: i16,
    pub pick_status: u16,
    pub time: u64,
    pub ban_heros: Vec<Vec<String>>,
}

#[derive(Clone, Debug)]
pub enum ATGameStatus {
    Loading,
    Ban,
    Pick,
    ReadyToStart,
    Gaming,
    Finished,
}

impl ATGame {
    pub fn check_status(&mut self) -> ATGameStatus {
        let mut res = ATGameStatus::Loading;
        if self.game_status == 0 {
            res = ATGameStatus::Loading;
        }
        if self.game_status == 1 {
            res = ATGameStatus::Ban;
        }
        if self.game_status == 2 {
            res = ATGameStatus::Ban;
        }
        if self.game_status == 3 {
            res = ATGameStatus::Ban;
        }
        if self.game_status == 4 {
            res = ATGameStatus::Ban;
        }
        if self.game_status == 5 {
            res = ATGameStatus::Ban;
        }
        if self.game_status == 6 {
            res = ATGameStatus::Ban;
        }
        if self.game_status == 7 {
            res = ATGameStatus::Pick;
        }
        if self.game_status == 8 {
            res = ATGameStatus::Pick;
        }
        if self.game_status == 9 {
            res = ATGameStatus::Pick;
        }
        if self.game_status == 10 {
            res = ATGameStatus::Pick;
        }
        if self.game_status == 11 {
            res = ATGameStatus::Pick;
        }
        if self.game_status == 12 {
            res = ATGameStatus::Pick;
        }
        if self.game_status == 13 {
            res = ATGameStatus::Ban;
        }
        if self.game_status == 14 {
            res = ATGameStatus::Ban;
        }
        if self.game_status == 15 {
            res = ATGameStatus::Ban;
        }
        if self.game_status == 16 {
            res = ATGameStatus::Ban;
        }
        if self.game_status == 17 {
            res = ATGameStatus::Pick;
        }
        if self.game_status == 18 {
            res = ATGameStatus::Pick;
        }
        if self.game_status == 19 {
            res = ATGameStatus::Pick;
        }
        if self.game_status == 20 {
            res = ATGameStatus::Pick;
        }
        if self.game_status == 21 {
            res = ATGameStatus::ReadyToStart;
        }
        if self.game_status == 22 {
            res = ATGameStatus::Gaming;
        }
        if self.game_status == 23 {
            res = ATGameStatus::Finished;
        }
        res
    }
    pub fn next_status(&mut self) {
        // let mut res = ATGameStatus::Loading;
        self.game_status += 1;
        info!(
            "AT game_id : {}, status: {}, line: {}",
            self.game_id,
            self.game_status,
            line!()
        );
        if self.game_status == 0 {
            // res = ATGameStatus::Loading;
        }
        if self.game_status == 1 {
            // res = ATGameStatus::Ban;
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![0];
        }
        if self.game_status == 2 {
            // res = ATGameStatus::Ban;
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![5];
        }
        if self.game_status == 3 {
            // res = ATGameStatus::Ban;
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![1];
        }
        if self.game_status == 4 {
            // res = ATGameStatus::Ban;
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![6];
        }
        if self.game_status == 5 {
            // res = ATGameStatus::Ban;
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![2];
        }
        if self.game_status == 6 {
            // res = ATGameStatus::Ban;
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![7];
        }
        if self.game_status == 7 {
            // res = ATGameStatus::Pick;
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![0];
        }
        if self.game_status == 8 {
            // res = ATGameStatus::Pick;
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![5];
        }
        if self.game_status == 9 {
            // res = ATGameStatus::Pick;
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![6];
        }
        if self.game_status == 10 {
            // res = ATGameStatus::Pick;
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![1];
        }
        if self.game_status == 11 {
            // res = ATGameStatus::Pick;
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![2];
        }
        if self.game_status == 12 {
            // res = ATGameStatus::Pick;
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![7];
        }
        if self.game_status == 13 {
            // res = ATGameStatus::Ban;
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![8];
        }
        if self.game_status == 14 {
            // res = ATGameStatus::Ban;
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![3];
        }
        if self.game_status == 15 {
            // res = ATGameStatus::Ban;
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![9];
        }
        if self.game_status == 16 {
            // res = ATGameStatus::Ban;
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![4];
        }
        if self.game_status == 17 {
            // res = ATGameStatus::Pick;
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![8];
        }
        if self.game_status == 18 {
            // res = ATGameStatus::Pick;
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![3];
        }
        if self.game_status == 19 {
            // res = ATGameStatus::Pick;
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![4];
        }
        if self.game_status == 20 {
            // res = ATGameStatus::Pick;
            self.choose_time = CHOOSE_HERO_TIME;
            self.pick_position = vec![9];
        }
        if self.game_status == 21 {
            self.ready_to_start_time = READY_TO_START_TIME;
            self.pick_position = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            // res = ATGameStatus::ReadyToStart;
        }
        if self.game_status == 22 {
            // res = ATGameStatus::Gaming;
        }
        if self.game_status == 23 {
            // res = ATGameStatus::Finished;
        }
        // res
    }
    pub fn check_loading(&mut self) -> bool {
        let mut res = true;
        for team in &self.teams {
            if !team.borrow_mut().check_loading() {
                res = false;
            }
        }
        res
    }
    pub fn check_lock(&mut self) -> bool {
        let mut res = true;
        for team in &self.teams {
            if !team.borrow_mut().check_lock() {
                res = false;
            }
        }
        res
    }
    pub fn updateBanHeros(&mut self) {
        let mut result: Vec<Vec<String>> = Vec::new();
        for team in &self.teams {
            let mut heros: Vec<String> = Vec::new();
            for hero in team.borrow_mut().get_users_ban_hero() {
                println!("{}", hero);
                heros.push(hero);
            }
            result.push(heros);
        }
        self.ban_heros = result;
    }
}
#[derive(Clone, Debug, Default)]
pub struct ARAMGame {
    pub teams: Vec<Rc<RefCell<FightGroup>>>,
    pub room_names: Vec<String>,
    pub user_names: Vec<String>,
    pub pick_position: Vec<usize>,
    pub game_id: u64,
    pub user_count: u16,
    pub winteam: i16,
    pub game_status: u16,
    pub ban_time: i16,
    pub choose_time: i16,
    pub ready_to_start_time: i16,
    pub pick_status: u16,
    pub time: u64,
    pub heros: Vec<String>,
    pub TotalHeros: BTreeMap<String, Rc<RefCell<HeroData>>>,
}

#[derive(Clone, Debug)]
pub enum ARAMGameStatus {
    Loading,
    Ban,
    Pick,
    ReadyToStart,
    Gaming,
    Finished,
}

impl ARAMGame {
    pub fn check_status(&mut self) -> ARAMGameStatus {
        let mut res = ARAMGameStatus::Loading;
        if self.game_status == 0 {
            res = ARAMGameStatus::Loading;
        }
        if self.game_status == 1 {
            res = ARAMGameStatus::Ban;
        }
        if self.game_status == 2 {
            res = ARAMGameStatus::Pick;
        }
        if self.game_status == 3 {
            res = ARAMGameStatus::ReadyToStart;
        }
        if self.game_status == 4 {
            res = ARAMGameStatus::Gaming;
        }
        if self.game_status == 5 {
            res = ARAMGameStatus::Finished;
        }
        res
    }
    pub fn next_status(&mut self) {
        let mut res = ARAMGameStatus::Loading;
        self.game_status += 1;
        info!(
            "NG game_id : {}, status: {}, status_code: {}, line: {}",
            self.game_id.clone(),
            self.get_status_name(),
            self.game_status,
            line!()
        );
        if self.game_status == 0 {}
        if self.game_status == 1 {
            self.ban_time = BAN_HERO_TIME;
            self.pick_position = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        }
        if self.game_status == 2 {
            self.choose_time = NG_CHOOSE_HERO_TIME;
            self.pick_position = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        }
        if self.game_status == 3 {
            self.ready_to_start_time = ARAM_READY_TO_START_TIME;
            self.pick_position = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        }
        if self.game_status == 4 {}
        if self.game_status == 5 {}
    }
    pub fn get_status_name(&mut self) -> String {
        let mut res = "loading";
        if self.game_status == 0 {
            res = "loading"
        }
        if self.game_status == 1 {
            res = "ban"
        }
        if self.game_status == 2 {
            res = "pick"
        }
        if self.game_status == 3 {
            res = "readyToStart";
        }
        if self.game_status == 4 {
            res = "gaming";
        }
        if self.game_status == 5 {
            res = "finished";
        }
        res.to_string()
    }
    pub fn check_loading(&mut self) -> bool {
        let mut res = true;
        for team in &self.teams {
            if !team.borrow_mut().check_loading() {
                res = false;
            }
        }
        res
    }
    pub fn check_lock(&mut self) -> bool {
        let mut res = true;
        for team in &self.teams {
            if !team.borrow_mut().check_lock() {
                res = false;
            }
        }
        res
    }
    pub fn rollHeros(&mut self) {
        let mut result: Vec<String> = Vec::new();
        let mut heros: Vec<String> = Vec::new();
        for (name, hero) in &self.TotalHeros {
            if hero.borrow().enable {
                heros.push(name.clone());
            }
        }
        for team in &self.teams {
            for room in &team.borrow().rooms {
                for user in &room.borrow().users {
                    let mut done: bool = false;
                    while !done {
                        let mut rng = thread_rng();
                        let mut hero: String = "".to_string();
                        match heros.iter().choose(&mut rng) {
                            None => println!("no hero, line: {}", line!()),
                            Some(h) => {
                                done = true;
                                hero = h.to_string();
                                user.borrow_mut().hero = hero.clone();
                            },
                        }
                        heros.retain(|h| *h != hero);
                    }
                }
            }
        }
        self.heros = result;
    }
}