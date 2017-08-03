use std::collections::VecDeque;
use std::slice;
use std::ptr;
use std::mem;
use std::any::Any;
use std::sync::mpsc::{self, Sender, Receiver};

fn alloc<T>(val: T) -> *mut T {
    Box::into_raw(Box::new(val))
}

pub enum ConcreteObject {
    Other(*mut Object),
    System(*mut System),
}

type Update = Box<Any + Send>;

pub trait RunContext {
    fn background(&mut self) -> Sender<Update>;
}

pub trait Runnable {
    fn run(&mut self, &mut RunContext);
    fn update(&mut self, &mut RunContext, Update) {
        unimplemented!();
    }
}

pub trait Object {
    fn name(&self) -> &'static str;
    fn runnable(&mut self) -> Option<&mut Runnable> {
        None
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

impl LinkEnd {
    fn frame(&self) -> *mut Frame {
        match self {
            &LinkEnd::Frame(frame) => frame,
            &LinkEnd::FrameElement(frame, _) => frame,
        }
    }
}

#[derive(Clone)]
struct Link {
    relation: Relation,
    a: LinkEnd,
    b: LinkEnd,
}

pub struct Frame {
    parent: *mut Object,
    name: String,
    object: Option<*mut Object>,
}

struct BackgroundTask {
    frame: *mut Frame,
    receiver: Receiver<Update>,
}

pub struct System {
    frame: Option<*mut Frame>,
    frames: Vec<*mut Frame>,
    tasks: VecDeque<*mut Frame>,
    links: Vec<Link>,
    background_tasks: Vec<BackgroundTask>,
}

impl RunContext for Frame {
    fn background(&mut self) -> Sender<Update> {
        unsafe {
            match (*self.parent).concrete() {
                ConcreteObject::System(system) => {
                    let (sender, receiver) = mpsc::channel();
                    (*system).background_tasks.push(BackgroundTask {
                        frame: self,
                        receiver,
                    });
                    return sender;
                }
                _ => unimplemented!(),
            }
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

impl Frame {
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
            tasks: VecDeque::new(),
            links: Vec::new(),
            background_tasks: Vec::new(),
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
    pub fn frame(&mut self, object: Option<Box<Object>>) -> *mut Frame {
        let frame = alloc(Frame {
            parent: self,
            name: String::new(),
            object: object.map(Box::into_raw),
        });
        self.pick_name(frame);
        unsafe {
            (*frame).maybe_update_frame();
        }
        self.frames.push(frame);
        return frame;
    }
    /// Adds frame to the task lisk regardless of whether it's already there.
    fn post(&mut self, frame: *mut Frame) {
        self.tasks.push_back(frame);
    }
    /// Checks if a frame is already on the task list.
    fn is_marked(&self, frame: *mut Frame) -> bool {
        for task in self.tasks.iter() {
            if *task == frame {
                return true;
            }
        }
        return false;
    }
    /// Marks the frame for execution.
    fn mark(&mut self, frame: *mut Frame) {
        unsafe {
            if let Some(mut system) = System::from_object((*frame).parent) {
                if !system.is_marked(frame) {
                    system.post(frame);
                    if let Some(parent) = system.frame {
                        self.mark(parent);
                    }
                }
            }
        }
    }
    pub fn run_until_done(&mut self) {
        while !self.tasks.is_empty() || !self.background_tasks.is_empty() {
            self.run_one();
        }
    }
    pub fn run_iterations(&mut self, iterations: u32) {
        for _ in 0..iterations {
            self.run_one();
        }
    }
    pub fn run_one(&mut self) {
        // TODO: Replace with multi-channel selection, once it's available
        //       (https://github.com/rust-lang/rust/issues/27800)

        let mut i = 0;
        while i < self.background_tasks.len() {
            let recv_result = self.background_tasks[i].receiver.try_recv();
            match recv_result {
                Ok(update) => {
                    let frame = self.background_tasks[i].frame;
                    let object = unsafe { (*frame).object };
                    let object = object.unwrap();
                    let runnable = unsafe { (*object).runnable() };
                    let runnable = runnable.unwrap();
                    runnable.update(unsafe { &mut *frame }, update);
                    return;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    i += 1;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    let frame = self.background_tasks[i].frame;
                    self.background_tasks.swap_remove(i);
                    self.run_finished(frame);
                }
            }
        }

        match self.tasks.pop_front() {
            Some(frame) => {
                self.run_frame(frame);
            }
            None => (),
        }
    }
    fn run_finished(&mut self, frame: *mut Frame) {
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
                LinkEnd::Frame(link_b) => {
                    self.mark(link_b);
                }
                LinkEnd::FrameElement(frame, element) => unsafe {
                    let target = (*frame).find_element(&element);
                    match target {
                        Some(frame) => {
                            self.mark(frame);
                        }
                        None => {
                            panic!("Element {} not found", element);
                        }
                    }
                },
            }
        }
        if let Some(parent) = self.parent_system() {
            parent.run_finished(frame);
        }
    }
    fn is_running(&self, frame: *mut Frame) -> bool {
        self.background_tasks.iter().any(|task| {
            ptr::eq(task.frame, frame)
        })
    }
    fn run_frame(&mut self, frame: *mut Frame) {
        unsafe {
            match (*frame).object {
                Some(object) => {
                    match (*object).runnable() {
                        Some(ref mut runnable) => {
                            runnable.run(&mut *frame);
                            if !self.is_running(frame) {
                                self.run_finished(frame);
                            }
                        }
                        _ => unimplemented!(),
                    }
                }
                _ => unimplemented!(),
            };
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
    fn runnable(&mut self) -> Option<&mut Runnable> {
        Some(self)
    }
    fn elements(&self) -> &[*mut Frame] {
        &self.frames
    }
}

impl Runnable for System {
    fn run(&mut self, _: &mut RunContext) {
        self.run_one();
        if !self.tasks.is_empty() || !self.background_tasks.is_empty() {
            if let Some(frame) = self.frame {
                self.mark(frame);
            }
        }
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
        fn runnable(&mut self) -> Option<&mut Runnable> {
            Some(self)
        }
        fn concrete(&mut self) -> ConcreteObject {
            ConcreteObject::Other(self)
        }
    }

    impl Runnable for MockObject {
        fn run(&mut self, _: &mut RunContext) {
            self.log.borrow_mut().push(self.name.clone());
        }
    }

    struct TestableSystem {
        system: Box<System>,
        a: *mut Frame,
        b: *mut Frame,
        c: *mut Frame,
    }

    struct Test {
        log: Log,
    }

    impl Test {
        fn new() -> Self {
            let log = Rc::new(RefCell::new(Vec::new()));
            Test { log }
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
        let test = Test::new();
        let TestableSystem { mut system, .. } = test.make_system("");
        system.run_until_done();
        assert_eq!(test.log(), "");
    }

    #[test]
    fn run_one() {
        let test = Test::new();
        let TestableSystem { mut system, a, .. } = test.make_system("");
        system.mark(a);
        system.run_until_done();
        assert_eq!(test.log(), ":a");
    }

    #[test]
    fn swap() {
        let test = Test::new();
        let TestableSystem { mut system, a, b, .. } = test.make_system("");
        Frame::swap(a, b);
        system.mark(a);
        system.run_until_done();
        assert_eq!(test.log(), ":b");
    }

    #[test]
    fn then() {
        let test = Test::new();
        let TestableSystem { mut system, a, b, .. } = test.make_system("");
        system.link(a, b, Relation::Then);
        system.mark(a);
        system.run_until_done();
        assert_eq!(test.log(), ":a :b");
    }

    #[test]
    fn test_loop() {
        let test = Test::new();
        let TestableSystem { mut system, a, .. } = test.make_system("");
        system.link(a, a, Relation::Then);
        system.mark(a);
        system.run_iterations(3);
        assert_eq!(test.log(), ":a :a :a");
    }

    #[test]
    fn split() {
        let test = Test::new();
        let TestableSystem {
            mut system,
            a,
            b,
            c,
        } = test.make_system("");
        system.link(a, b, Relation::Then);
        system.link(a, c, Relation::Then);
        system.mark(a);
        system.run_until_done();
        assert_eq!(test.log(), ":a :b :c");
    }

    #[test]
    fn merge() {
        let test = Test::new();
        let TestableSystem {
            mut system,
            a,
            b,
            c,
        } = test.make_system("");
        system.link(a, c, Relation::Then);
        system.link(b, c, Relation::Then);
        system.mark(a);
        system.mark(b);
        system.run_until_done();
        assert_eq!(test.log(), ":a :b :c");
    }

    #[test]
    fn repeat() {
        let test = Test::new();
        let TestableSystem {
            mut system,
            a,
            b,
            c,
        } = test.make_system("");
        system.link(a, c, Relation::Then);
        system.link(b, c, Relation::Then);
        system.mark(a);
        system.run_until_done();
        system.mark(b);
        system.run_until_done();
        assert_eq!(test.log(), ":a :c :b :c");
    }

    // Cross-system running tests:

    struct CrossSystemTest {
        test: Test,
        system: Box<System>,
        top: *mut Frame,
        left: *mut Frame,
        right: *mut Frame,
        left_system: *mut Frame,
        right_system: *mut Frame,
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
            unsafe {
                (*top.b).adopt(Some(left_system));
                (*top.c).adopt(Some(right_system));
            }
            CrossSystemTest {
                test,
                system: top.system,
                top: top.a,
                left: left_a,
                left_system: top.b,
                right: right_a,
                right_system: top.c,
            }
        }
    }

    #[test]
    fn enter_system() {
        let CrossSystemTest {
            test,
            mut system,
            top,
            left,
            ..
        } = CrossSystemTest::new();

        system.link(top, left, Relation::Then);
        system.mark(top);
        system.run_iterations(2);

        assert_eq!(test.log(), "Top:a Left:a");
    }

    #[test]
    fn exit_system() {
        let CrossSystemTest {
            test,
            mut system,
            top,
            left,
            ..
        } = CrossSystemTest::new();

        system.link(left, top, Relation::Then);
        system.mark(left);
        system.run_iterations(2);

        assert_eq!(test.log(), "Left:a Top:a");
    }

    #[test]
    fn cross_systems() {
        let CrossSystemTest {
            test,
            mut system,
            left,
            right,
            left_system,
            right_system,
            ..
        } = CrossSystemTest::new();

        system.link(left, right, Relation::Then);
        system.link(right, left, Relation::Then);
        system.mark(left);
        System::from_frame(left_system).unwrap().run_until_done();
        System::from_frame(right_system).unwrap().run_until_done();
        system.run_iterations(2);

        assert_eq!(test.log(), "Left:a Right:a Left:a Right:a");
    }

    #[test]
    fn system_substitution() {
        let test = Test::new();
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
        system.mark(top);
        system.run_until_done();

        assert_eq!(test.log(), "top left3");

        Frame::swap(left1, right1);
        system.mark(top);
        system.run_until_done();

        assert_eq!(test.log(), "top left3 top right3");

        Frame::swap(left2, right2);
        system.mark(top);
        system.run_until_done();

        assert_eq!(test.log(), "top left3 top right3 top left3");

        Frame::swap(left3, right3);
        system.mark(top);
        system.run_until_done();

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
        fn runnable(&mut self) -> Option<&mut Runnable> {
            Some(self)
        }
        fn concrete(&mut self) -> ConcreteObject {
            ConcreteObject::Other(self)
        }
    }

    impl Runnable for SlowObject {
        fn run(&mut self, context: &mut RunContext) {
            use std::{thread, time};
            self.0.borrow_mut().push("start".to_string());
            let sender = context.background();
            thread::spawn(move || {
                thread::sleep(time::Duration::from_millis(10));
                sender.send(Box::new(())).unwrap();
            });
        }
        fn update(&mut self, _: &mut RunContext, _: Update) {
            self.0.borrow_mut().push("end".to_string());
        }
    }

    #[test]
    fn background() {
        let test = Test::new();
        let mut system = System::new();
        let slow = system.frame(Some(SlowObject::new(&test.log)));
        let then = system.frame(Some(MockObject::new("mock".to_string(), &test.log)));
        system.link(slow, then, Relation::Then);
        system.mark(slow);
        system.run_until_done();

        assert_eq!(test.log(), "start end mock");
    }
}
