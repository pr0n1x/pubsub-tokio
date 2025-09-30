use core::{convert::AsRef, fmt, hash::Hash, ops::Deref};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Ro<T>(T);

impl<T> Ro<T> {
    pub fn new(inner: T) -> Self {
        Ro(inner)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> AsRef<T> for Ro<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> Deref for Ro<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: fmt::Display> fmt::Display for Ro<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
