use pg_extend::pg_magic;

pg_magic!(version: pg_sys::GP_VERSION_NUM);

mod foreign_data_wrappers;
mod instance;
