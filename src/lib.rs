use std::collections::VecDeque;
use std::slice;
use std::ptr;
use std::mem;
use std::collections::HashMap;
use std::any::Any;
use std::sync::mpsc::{self, Sender, Receiver};

pub trait Object {
    fn name(&self) -> &'static str;
    fn can_run(&self) -> bool {
        false
    }
    fn run(&mut self, _: RunContext) {
        unimplemented!()
    }
    fn update(&mut self, _: Update) {
        unimplemented!()
    }
    fn concrete(&mut self) -> ConcreteObject;
    fn deserialize(&mut self, Vec<u8>) {}
    fn serialize(&self) -> Vec<u8> {
        Vec::new()
    }
    fn elements(&self) -> &[*mut Frame] {
        unsafe { slice::from_raw_parts(ptr::null(), 0) }
    }
}

pub trait FrameData {
    fn new() -> Self;
}

pub struct System {
    frame: Option<*mut Frame>,
    frames: Vec<*mut Frame>,
    links: Vec<Link>,
}

pub struct Frame {
    parent: *mut Object,
    name: String,
    object: Option<*mut Object>,
    scheduled: bool,
    running: bool,
}

pub enum ConcreteObject {
    Other(*mut Object),
    System(*mut System),
}

#[derive(Clone)]
struct Link {
    relation: Relation,
    a: LinkEnd,
    b: LinkEnd,
}

#[derive(Clone)]
pub enum Relation {
    Then,
    Arg,
}

#[derive(Clone)]
enum LinkEnd {
    Frame(*mut Frame),
    FrameElement(*mut Frame, String),
}

type Update = Box<Any + Send>;

enum TaskEvent {
    Update(Update),
    Drop,
}

pub struct TaskLoop {
    counter: u64,
    background: HashMap<u64, Task>,
    tx: Sender<(u64, TaskEvent)>,
    rx: Receiver<(u64, TaskEvent)>,
    tasks: VecDeque<Task>,
}

pub struct BackgroundTask {
    id: u64,
    tx: Sender<(u64, TaskEvent)>,
}

struct Task {
    frame: *mut Frame,
}

pub struct RunContext<'a> {
    task: Option<Task>,
    task_loop: &'a mut TaskLoop,
}

impl LinkEnd {
    fn frame(&self) -> *mut Frame {
        match self {
            &LinkEnd::Frame(frame) => frame,
            &LinkEnd::FrameElement(frame, _) => frame,
        }
    }
}

fn find_element(object: *const Object, name: &String) -> Option<*mut Frame> {
    let mut deq: VecDeque<*mut Frame> = VecDeque::new();
    unsafe {
        deq.extend((*object).elements());
        while let Some(frame) = deq.pop_front() {
            let frame_name: &String = &(*frame).name;
            if frame_name == name {
                return Some(frame);
            }
            if let Some(object) = (*frame).object {
                deq.extend((*object).elements());
            }
        }
    }
    return None;
}

impl TaskLoop {
    pub fn new() -> TaskLoop {
        let (tx, rx) = mpsc::channel();
        TaskLoop {
            counter: 0,
            background: HashMap::new(),
            tx,
            rx,
            tasks: VecDeque::new(),
        }
    }
    fn background(&mut self, task: Task) -> BackgroundTask {
        self.counter += 1;
        self.background.insert(self.counter, task);
        return BackgroundTask {
            id: self.counter,
            tx: self.tx.clone(),
        };
    }
    fn post(&mut self, task: Task) {
        self.tasks.push_back(task);
    }
    pub fn run_iterations(&mut self, n: u32) {
        for _ in 0..n {
            self.run_one();
        }
    }
    pub fn run_until_done(&mut self) {
        while self.run_one() {}
    }
    pub fn run_one(&mut self) -> bool {
        match self.rx.try_recv() {
            Ok((id, TaskEvent::Update(update))) => {
                self.background.get_mut(&id).unwrap().update(update);
                true
            }
            Ok((id, TaskEvent::Drop)) => {
                let task = self.background.remove(&id).unwrap();
                task.finish(self);
                true
            }
            _ => {
                match self.tasks.pop_front() {
                    Some(task) => {
                        task.run(self);
                        true
                    }
                    None => {
                        if self.background.is_empty() {
                            false
                        } else {
                            match self.rx.recv() {
                                Ok((id, TaskEvent::Update(update))) => {
                                    self.background.get_mut(&id).unwrap().update(update);
                                    true
                                }
                                Ok((id, TaskEvent::Drop)) => {
                                    let task = self.background.remove(&id).unwrap();
                                    task.finish(self);
                                    true
                                }
                                _ => panic!(),
                            }
                        }
                    }
                }
            }
        }
    }
}

impl BackgroundTask {
    pub fn send_update(&mut self, update: Update) {
        self.tx.send((self.id, TaskEvent::Update(update))).unwrap();
    }
}

impl Drop for BackgroundTask {
    fn drop(&mut self) {
        self.tx.send((self.id, TaskEvent::Drop)).unwrap();
    }
}

impl<'a> RunContext<'a> {
    pub fn background(mut self) -> BackgroundTask {
        return self.task_loop.background(self.task.take().unwrap());
    }
}

impl<'a> Drop for RunContext<'a> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.finish(self.task_loop);
        }
    }
}

impl Task {
    // Executed by TaskLoop
    fn run(self, task_loop: &mut TaskLoop) {
        unsafe {
            (*self.frame).scheduled = false;
            (*self.frame).running = true;
            match (*self.frame).object {
                Some(object) => {
                    (*object).run(RunContext {
                        task: Some(self),
                        task_loop: task_loop,
                    });
                }
                None => unimplemented!(),
            }
        }
    }
    // Executed by TaskLoop
    fn update(&mut self, update: Update) {
        unsafe {
            match (*self.frame).object {
                Some(object) => {
                    (*object).update(update);
                }
                None => unimplemented!(),
            }
        }
    }
    // Executed by TaskLoop
    fn finish(self, task_loop: &mut TaskLoop) {
        unsafe {
            (*self.frame).running = false;
            let system = (*self.frame).parent_system().unwrap();
            system.run_finished(self.frame, task_loop);
        }
    }
}

fn alloc<T>(val: T) -> *mut T {
    Box::into_raw(Box::new(val))
}

impl Frame {
    fn schedule(&mut self, task_loop: &mut TaskLoop) {
        if !self.scheduled {
            self.scheduled = true;
            task_loop.post(Task { frame: self });
        }
    }

    pub fn adopt(&mut self, adopted: Option<Box<Object>>) {
        self.object = adopted.map(Box::into_raw);
        self.maybe_update_frame();
    }
    fn maybe_update_frame(&mut self) {
        let object = self.object;
        if let Some(object) = object {
            unsafe {
                if let ConcreteObject::System(system) = (*object).concrete() {
                    (*system).frame = Some(self);
                }
            }
        }
    }
    fn parent_system(&self) -> Option<&'static mut System> {
        return System::from_object(self.parent);
    }
    fn find_element(&mut self, name: &String) -> Option<*mut Frame> {
        if let Some(object) = self.object {
            return find_element(object, name);
        }
        return None;
    }
    pub fn swap(a: *mut Frame, b: *mut Frame) {
        unsafe {
            if let Some(a) = System::from_frame(a) {
                a.break_links();
            }
            if let Some(b) = System::from_frame(b) {
                b.break_links();
            }
            mem::swap(&mut (*a).object, &mut (*b).object);
            (*a).maybe_update_frame();
            (*b).maybe_update_frame();
            if let Some(a) = System::from_frame(a) {
                a.fix_links();
            }
            if let Some(b) = System::from_frame(b) {
                b.fix_links();
            }
        }
    }
}

impl System {
    pub fn new() -> Box<System> {
        Box::new(System {
            frame: None,
            frames: Vec::new(),
            links: Vec::new(),
        })
    }
    fn from_object(object: *mut Object) -> Option<&'static mut System> {
        unsafe {
            match (*object).concrete() {
                ConcreteObject::System(system) => return Some(&mut *system),
                _ => None,
            }
        }
    }
    fn from_frame(frame: *mut Frame) -> Option<&'static mut System> {
        unsafe {
            if let Some(object) = (*frame).object {
                System::from_object(object)
            } else {
                None
            }
        }
    }
    fn parent_system(&self) -> Option<&'static mut System> {
        if let Some(my_frame) = self.frame {
            unsafe {
                return (*my_frame).parent_system();
            }
        }
        return None;
    }
    fn pick_name(&self, frame: *mut Frame) {
        let object = unsafe { (*frame).object };
        let base = match object {
            Some(object) => unsafe { (*object).name().to_string() },
            None => "Frame".to_string(),
        };
        let name = if let Some(_) = find_element(self, &base) {
            let mut counter = 2;
            let mut candidate = base.clone() + &counter.to_string();
            while let Some(_) = find_element(self, &candidate) {
                counter += 1;
                candidate = base.clone() + &counter.to_string();
            }
            candidate
        } else {
            base
        };
        unsafe {
            (*frame).name = name;
        }
    }
    pub fn frame(&mut self, object: Option<Box<Object>>) -> &'static mut Frame {
        let frame = alloc(Frame {
            parent: self,
            name: String::new(),
            object: object.map(Box::into_raw),
            running: false,
            scheduled: false,
        });
        self.pick_name(frame);
        unsafe {
            (*frame).maybe_update_frame();
        }
        self.frames.push(frame);
        return unsafe { &mut *frame };
    }
    fn run_finished(&mut self, frame: *mut Frame, task_loop: &mut TaskLoop) {
        for link in self.links.clone().into_iter() {
            match link.relation {
                Relation::Then => {}
                _ => continue,
            }
            match link.a {
                LinkEnd::Frame(link_a) => {
                    if link_a != frame {
                        continue;
                    }
                }
                _ => continue,
            }
            match link.b {
                LinkEnd::Frame(link_b) => unsafe {
                    (*link_b).schedule(task_loop);
                },
                LinkEnd::FrameElement(frame, element) => unsafe {
                    let target = (*frame).find_element(&element);
                    match target {
                        Some(frame) => {
                            (*frame).schedule(task_loop);
                        }
                        None => {
                            panic!("Element {} not found", element);
                        }
                    }
                },
            }
        }
        if let Some(parent) = self.parent_system() {
            parent.run_finished(frame, task_loop);
        }
    }
    fn contains(&self, frame: *mut Frame) -> bool {
        unsafe {
            if let Some(other) = (*frame).parent_system() {
                if ptr::eq(other, self) {
                    return true;
                }
                if let Some(higher_frame) = other.frame {
                    return self.contains(higher_frame);
                }
            }
            return false;
        }
    }
    fn break_link_end(&mut self, link_end: &mut LinkEnd) {
        let frame = link_end.frame();
        if self.contains(frame) {
            println!("Cutting a link!");
            let name: String = unsafe { (*frame).name.clone() };
            *link_end = LinkEnd::FrameElement(self.frame.unwrap(), name);
        }
    }
    fn break_link(&mut self, link: &mut Link) {
        self.break_link_end(&mut link.a);
        self.break_link_end(&mut link.b);
    }
    fn break_links(&mut self) {
        let mut frame = self.frame.unwrap();
        unsafe {
            while let Some(mut parent) = (*frame).parent_system() {
                for mut link in parent.links.iter_mut() {
                    self.break_link(link);
                }
                if let Some(x) = parent.frame {
                    frame = x;
                } else {
                    break;
                }
            }
        }
    }
    fn fix_link_end(&mut self, link_end: &mut LinkEnd) {
        let my_frame = self.frame.unwrap();
        if let LinkEnd::FrameElement(frame, element) = link_end.clone() {
            if ptr::eq(frame, my_frame) {
                let target = unsafe { (*my_frame).find_element(&element) };
                if let Some(target) = target {
                    *link_end = LinkEnd::Frame(target);
                }
            }
        }
    }
    fn fix_link(&mut self, link: &mut Link) {
        self.fix_link_end(&mut link.a);
        self.fix_link_end(&mut link.b);
    }
    fn fix_links(&mut self) {
        let mut frame = self.frame.unwrap();
        unsafe {
            while let Some(mut parent) = (*frame).parent_system() {
                for mut link in parent.links.iter_mut() {
                    self.fix_link(link);
                }
                if let Some(x) = parent.frame {
                    frame = x;
                } else {
                    break;
                }
            }
        }

    }
    pub fn link(&mut self, a: *mut Frame, b: *mut Frame, relation: Relation) {
        self.links.push(Link {
            relation,
            a: LinkEnd::Frame(a),
            b: LinkEnd::Frame(b),
        });
    }
}

impl Object for System {
    fn name(&self) -> &'static str {
        "System"
    }
    fn concrete(&mut self) -> ConcreteObject {
        ConcreteObject::System(self)
    }
    fn elements(&self) -> &[*mut Frame] {
        &self.frames
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::cell::RefCell;
    use super::*;

    type Log = Rc<RefCell<Vec<String>>>;

    struct MockObject {
        name: String,
        log: Log,
    }

    impl MockObject {
        fn new(name: String, log: &Log) -> Box<Self> {
            Box::new(MockObject {
                name,
                log: log.clone(),
            })
        }
    }

    impl Object for MockObject {
        fn name(&self) -> &'static str {
            "MockObject"
        }
        fn can_run(&self) -> bool {
            true
        }
        fn run(&mut self, _: RunContext) {
            self.log.borrow_mut().push(self.name.clone());
        }
        fn concrete(&mut self) -> ConcreteObject {
            ConcreteObject::Other(self)
        }
    }

    struct TestableSystem {
        system: Box<System>,
        a: &'static mut Frame,
        b: &'static mut Frame,
        c: &'static mut Frame,
    }

    struct Test {
        log: Log,
        task_loop: TaskLoop,
    }

    impl Test {
        fn new() -> Self {
            let log = Rc::new(RefCell::new(Vec::new()));
            Test {
                log,
                task_loop: TaskLoop::new(),
            }
        }
        fn make_system(&self, name: &'static str) -> TestableSystem {
            let mut system = System::new();
            let name = name.to_string();
            let a = system.frame(Some(MockObject::new(name.clone() + ":a", &self.log)));
            let b = system.frame(Some(MockObject::new(name.clone() + ":b", &self.log)));
            let c = system.frame(Some(MockObject::new(name + ":c", &self.log)));
            return TestableSystem { system, a, b, c };
        }
        fn log(&self) -> String {
            self.log.borrow().join(" ")
        }
    }

    #[test]
    fn run_nothing() {
        let mut test = Test::new();
        test.make_system("");
        test.task_loop.run_until_done();
        assert_eq!(test.log(), "");
    }

    #[test]
    fn run_one() {
        let mut test = Test::new();
        let TestableSystem { a, .. } = test.make_system("");
        a.schedule(&mut test.task_loop);
        test.task_loop.run_until_done();
        assert_eq!(test.log(), ":a");
    }

    #[test]
    fn swap() {
        let mut test = Test::new();
        let TestableSystem { a, b, .. } = test.make_system("");
        Frame::swap(a, b);
        a.schedule(&mut test.task_loop);
        test.task_loop.run_until_done();
        assert_eq!(test.log(), ":b");
    }

    #[test]
    fn then() {
        let mut test = Test::new();
        let TestableSystem { mut system, a, b, .. } = test.make_system("");
        system.link(a, b, Relation::Then);
        a.schedule(&mut test.task_loop);
        test.task_loop.run_until_done();
        assert_eq!(test.log(), ":a :b");
    }

    #[test]
    fn test_loop() {
        let mut test = Test::new();
        let TestableSystem { mut system, a, .. } = test.make_system("");
        system.link(a, a, Relation::Then);
        a.schedule(&mut test.task_loop);
        test.task_loop.run_iterations(3);
        assert_eq!(test.log(), ":a :a :a");
    }

    #[test]
    fn split() {
        let mut test = Test::new();
        let TestableSystem {
            mut system,
            a,
            b,
            c,
        } = test.make_system("");
        system.link(a, b, Relation::Then);
        system.link(a, c, Relation::Then);
        a.schedule(&mut test.task_loop);
        test.task_loop.run_until_done();
        assert_eq!(test.log(), ":a :b :c");
    }

    #[test]
    fn merge() {
        let mut test = Test::new();
        let TestableSystem {
            mut system,
            a,
            b,
            c,
        } = test.make_system("");
        system.link(a, c, Relation::Then);
        system.link(b, c, Relation::Then);
        a.schedule(&mut test.task_loop);
        b.schedule(&mut test.task_loop);
        test.task_loop.run_until_done();
        assert_eq!(test.log(), ":a :b :c");
    }

    #[test]
    fn repeat() {
        let mut test = Test::new();
        let TestableSystem {
            mut system,
            a,
            b,
            c,
        } = test.make_system("");
        system.link(a, c, Relation::Then);
        system.link(b, c, Relation::Then);
        a.schedule(&mut test.task_loop);
        test.task_loop.run_until_done();
        b.schedule(&mut test.task_loop);
        test.task_loop.run_until_done();
        assert_eq!(test.log(), ":a :c :b :c");
    }

    // Cross-system running tests:

    struct CrossSystemTest {
        test: Test,
        system: Box<System>,
        top: &'static mut Frame,
        left: &'static mut Frame,
        right: &'static mut Frame,
    }

    impl CrossSystemTest {
        fn new() -> Self {
            let test = Test::new();
            let top = test.make_system("Top");
            let TestableSystem {
                system: left_system,
                a: left_a,
                ..
            } = test.make_system("Left");
            let TestableSystem {
                system: right_system,
                a: right_a,
                ..
            } = test.make_system("Right");
            top.b.adopt(Some(left_system));
            top.c.adopt(Some(right_system));
            CrossSystemTest {
                test,
                system: top.system,
                top: top.a,
                left: left_a,
                right: right_a,
            }
        }
    }

    #[test]
    fn enter_system() {
        let CrossSystemTest {
            mut test,
            mut system,
            top,
            left,
            ..
        } = CrossSystemTest::new();

        system.link(top, left, Relation::Then);
        top.schedule(&mut test.task_loop);
        test.task_loop.run_iterations(2);

        assert_eq!(test.log(), "Top:a Left:a");
    }

    #[test]
    fn exit_system() {
        let CrossSystemTest {
            mut test,
            mut system,
            top,
            left,
            ..
        } = CrossSystemTest::new();

        system.link(left, top, Relation::Then);
        left.schedule(&mut test.task_loop);
        test.task_loop.run_iterations(2);

        assert_eq!(test.log(), "Left:a Top:a");
    }

    #[test]
    fn cross_systems() {
        let CrossSystemTest {
            mut test,
            mut system,
            left,
            right,
            ..
        } = CrossSystemTest::new();

        system.link(left, right, Relation::Then);
        system.link(right, left, Relation::Then);
        left.schedule(&mut test.task_loop);
        test.task_loop.run_iterations(4);

        assert_eq!(test.log(), "Left:a Right:a Left:a Right:a");
    }

    #[test]
    fn system_substitution() {
        let mut test = Test::new();
        let mut system = System::new();

        let top = system.frame(Some(MockObject::new("top".to_string(), &test.log)));

        let left1 = system.frame(Some(System::new()));
        let left2 = System::from_frame(left1).unwrap().frame(
            Some(System::new()),
        );
        let left3 = System::from_frame(left2).unwrap().frame(
            Some(MockObject::new(
                "left3".to_string(),
                &test.log,
            )),
        );

        let right1 = system.frame(Some(System::new()));
        let right2 = System::from_frame(right1).unwrap().frame(
            Some(System::new()),
        );
        let right3 = System::from_frame(right2).unwrap().frame(
            Some(MockObject::new(
                "right3".to_string(),
                &test.log,
            )),
        );

        system.link(top, left3, Relation::Then);
        top.schedule(&mut test.task_loop);
        test.task_loop.run_until_done();

        assert_eq!(test.log(), "top left3");

        Frame::swap(left1, right1);
        top.schedule(&mut test.task_loop);
        test.task_loop.run_until_done();

        assert_eq!(test.log(), "top left3 top right3");

        Frame::swap(left2, right2);
        top.schedule(&mut test.task_loop);
        test.task_loop.run_until_done();

        assert_eq!(test.log(), "top left3 top right3 top left3");

        Frame::swap(left3, right3);
        top.schedule(&mut test.task_loop);
        test.task_loop.run_until_done();

        assert_eq!(test.log(), "top left3 top right3 top left3 top right3");
    }

    struct SlowObject(Log);

    impl SlowObject {
        fn new(log: &Log) -> Box<Self> {
            Box::new(SlowObject(log.clone()))
        }
    }

    impl Object for SlowObject {
        fn name(&self) -> &'static str {
            "SlowObject"
        }
        fn concrete(&mut self) -> ConcreteObject {
            ConcreteObject::Other(self)
        }
        fn can_run(&self) -> bool {
            true
        }
        fn run(&mut self, ctx: RunContext) {
            use std::{thread, time};
            self.0.borrow_mut().push("start".to_string());
            let mut background = ctx.background();
            thread::spawn(move || {
                thread::sleep(time::Duration::from_millis(10));
                background.send_update(Box::new(()));
            });
        }
        fn update(&mut self, _: Update) {
            self.0.borrow_mut().push("end".to_string());
        }
    }

    #[test]
    fn background() {
        let mut test = Test::new();
        let mut system = System::new();
        let slow = system.frame(Some(SlowObject::new(&test.log)));
        let then = system.frame(Some(MockObject::new("mock".to_string(), &test.log)));
        system.link(slow, then, Relation::Then);
        slow.schedule(&mut test.task_loop);
        test.task_loop.run_until_done();

        assert_eq!(test.log(), "start end mock");
    }
}
