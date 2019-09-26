
struct EloRank {
    k: f32,
}

pub fn mean(numbers: &Vec<i32>) -> f32 {
    let sum: i32 = numbers.iter().sum();
    sum as f32 / numbers.len() as f32
}

pub fn median(numbers: &mut Vec<i32>) -> i32 {
    numbers.sort();
    let mid = numbers.len() / 2;
    if numbers.len() % 2 == 0 {
        mean(&vec![numbers[mid - 1], numbers[mid]]) as i32
    } else {
        numbers[mid]
    }
}

impl EloRank {
    pub fn get_expected(&self, a: f32, b: f32) -> f32 {
        return 1.0/(1.0+10f32.powf((b-a)/400f32));
    }
    pub fn rating(&self, expected: f32, actual: f32, current: f32) -> f32 {
        return (current+ self.k*(actual-expected)).round();
    }
    pub fn compute_elo(&self, win: i32, lose: i32)
        -> (i32, i32) {
        let ewin = self.get_expected(win as f32, lose as f32);
        let elose = self.get_expected(lose as f32, win as f32);
        let rwin = self.rating(ewin as f32, 1.0, win as f32);
        let rlose = self.rating(elose as f32, 0.0, lose as f32);
        (rwin as i32, rlose as i32)
    }
    
    pub fn compute_elo_team(&self, winteam: &Vec<i32>, loseteam: &Vec<i32>)
        -> (Vec<i32>, Vec<i32>) {
        let win = mean(winteam);
        let lose = mean(loseteam);
        let mut wint = vec![];
        let mut loset = vec![];
        for score in winteam {
            let ewin = self.get_expected(*score as f32, lose as f32);
            let rwin = self.rating(ewin as f32, 1.0, *score as f32);
            wint.push(rwin as i32);
        }
        for score in loseteam {
            let elose = self.get_expected(*score as f32, win as f32);
            let rlose = self.rating(elose as f32, 0.0, *score as f32);
            loset.push(rlose as i32);
        }
        (wint, loset)
    }
    pub fn compute_elo_battle_ground(&self, team: &Vec<i32>, win_mount: usize, scale: f32)
        -> Vec<i32> {
        let m = mean(team);
        let mut rest = vec![];
        let mut a = win_mount as f32 *scale + 0.25;
        for (i, score) in team.iter().enumerate() {
            let ewin = self.get_expected(*score as f32, m as f32);
            let rwin = self.rating(ewin as f32, a, *score as f32);
            a -= scale;
            rest.push(rwin as i32);
        }
        rest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_elo() {
        let elo = EloRank {k:20.0};
        let (w, l) = elo.compute_elo(1000, 1000);
        println!("win 1000, lose 1000 => {}, {}", w, l);
        
        let (w, l) = elo.compute_elo(1500, 1000);
        println!("win 1500, lose 1000 => {}, {}", w, l);

        let wint = vec![1200,1210,1190,1230,1250];
        let loset = vec![1150,1130,1120,1140,1170];
        let (wt, lt) = elo.compute_elo_team(&wint, &loset);
        println!("win {:?}, lose {:?} \n => {:?},      {:?}", wint, loset, wt, lt);

        let wint = vec![1000,980,990,1010,1020,1005,995,990];
        let rt = elo.compute_elo_battle_ground(&wint, 4, 0.4);
        println!("battle 0.4 {:?} \n =>        {:?}", wint, rt);
        let rt = elo.compute_elo_battle_ground(&wint, 4, 0.5);
        println!("battle 0.5 {:?} \n =>        {:?}", wint, rt);
        let rt = elo.compute_elo_battle_ground(&wint, 4, 0.6);
        println!("battle 0.6 {:?} \n =>        {:?}", wint, rt);
    }
}
