use std::collections::HashMap;
use std::fmt::Debug;

use oceanraft::multiraft::ApplyNormal;


#[derive(Default)]
struct Commands(HashMap<u64, Vec<Vec<u8>>>);

impl Commands {
    fn insert(&mut self, group_id: u64, data: Vec<u8>) {
        match self.0.get_mut(&group_id) {
            Some(cmds) => cmds.push(data),
            None => {
                self.0.insert(group_id, vec![data]);
            }
        }
    }
}

#[derive(Default,Debug)]
pub struct WriteChecker {
    writes: Commands,
    applys: Commands,
}

impl WriteChecker {
    pub fn insert_write(&mut self, group_id: u64, data: Vec<u8>) {
       self.writes.insert(group_id, data);
    }

    pub fn check(&mut self, applys: &Vec<ApplyNormal>) {
        self.fill_applys(applys);
        assert_eq!(self.writes, self.applys)
    }

    fn fill_applys(&mut self, applys: &Vec<ApplyNormal>) {
        for apply in applys.iter() {
            self.applys.insert(apply.group_id, apply.entry.data.clone());
        }
    }
}

impl Debug for Commands {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = write!(f, "group_size = {}, [", self.0.len())?;
        for (group_id, commands) in self.0.iter() {
            let _ = write!(f, "{}: commands = {}, ", *group_id, commands.len())?;
        }
        write!(f, "]")
    }
}


impl PartialEq for Commands {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }

        for (group_id, commands) in self.0.iter() {
            if let Some(other_commands) = other.0.get(group_id) {
                if commands.len() != other_commands.len() {
                    return false;
                }

                for (c1, c2) in commands.iter().zip(other_commands) {
                    if c1 != c2 {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }

        true
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}
