use pg_extend::{debug, error};
use pg_extern_attr::pg_extern;
use std::{collections::HashMap, fs::File, io::prelude::*, sync::RwLock};
use uuid::Uuid;
use wasmer::{imports, Instance, Module, Store, Type, Value};

pub(crate) struct InstanceInfo {
    pub(crate) instance: Instance,
    pub(crate) wasm_file: String,
}

static mut INSTANCES: Option<RwLock<HashMap<String, InstanceInfo>>> = None;

pub(crate) fn get_instances() -> &'static RwLock<HashMap<String, InstanceInfo>> {
    unsafe {
        if INSTANCES.is_none() {
            let lock = RwLock::new(HashMap::new());
            INSTANCES = Some(lock);
        }

        &INSTANCES.as_ref().unwrap()
    }
}

#[pg_extern]
fn new_instance(wasm_file: String) -> Option<String> {
    let mut file = match File::open(&wasm_file) {
        Ok(file) => file,
        Err(e) => {
            error!("error opening {} - {}", &wasm_file, e);
            return None
        },
    };

    debug!("opened WASM file {}", &wasm_file);

    let mut bytes = Vec::new();

    if let Err(e) = file.read_to_end(&mut bytes) {
        error!("error reading {} - {}", &wasm_file, e);
        return None;
    }

    debug!("read WASM file {}", &wasm_file);

    let store = Store::default();
    let module = Module::new(&store, &bytes).unwrap();

    debug!("created module for WASM file {}", &wasm_file);

    let import_object = imports! {};
    match Instance::new(&module, &import_object) {
        Ok(instance) => {
            let mut instances = get_instances().write().unwrap();
            let key = Uuid::new_v5(
                &Uuid::NAMESPACE_OID,
                wasmer_cache::Hash::generate(bytes.as_slice()).to_string().as_bytes()
            )
            .to_hyphenated()
            .to_string();

            debug!("adding instance with key {}", &key);

            instances.insert(
                key.clone(),
                InstanceInfo {
                    instance,
                    wasm_file,
                },
            );

            Some(key)
        },
        Err(e) => {
            error!("error instantiating instance from {} - {}", &wasm_file, e);
            None
        },
    }
}

fn invoke_function(instance_id: String, function_name: String, arguments: &[i64]) -> Option<i64> {
    let instances = get_instances().read().unwrap();

    match instances.get(&instance_id) {
        Some(InstanceInfo { instance, .. }) => {
            let function = match instance.exports.get_function(&function_name) {
                Ok(function) => function,
                Err(error) => {
                    error!(
                        "Exported function `{}` does not exist in instance `{}`: {}",
                        function_name, instance_id, error
                    );

                    return None;
                }
            };

            let signature = function.ty();
            let parameters = signature.params();
            let number_of_parameters = parameters.len() as isize;
            let number_of_arguments = arguments.len() as isize;
            let diff: isize = number_of_parameters - number_of_arguments;

            if diff != 0 {
                error!(
                    "Failed to call the `{}` exported function of instance `{}`: Invalid number of arguments.",
                    function_name, instance_id
                );

                return None;
            }

            let mut function_arguments = Vec::<Value>::with_capacity(number_of_parameters as usize);

            for (parameter, argument) in parameters.iter().zip(arguments.iter()) {
                let value = match parameter {
                    Type::I32 => Value::I32(*argument as i32),
                    Type::I64 => Value::I64(*argument),
                    _ => {
                        error!(
                            "Failed to call the `{}` exported function of instance `{}`: Cannot call it because one of its argument expect a float (`f32` or `f64`), and it is not supported yet by the Postgres extension.",
                            function_name, instance_id
                        );

                        return None;
                    }
                };

                function_arguments.push(value);
            }

            let results = match function.call(function_arguments.as_slice()) {
                Ok(results) => results,
                Err(error) => {
                    error!(
                        "Failed to call the `{}` exported function of instance `{}`: {}",
                        function_name, instance_id, error
                    );

                    return None;
                }
            };

            if results.len() == 1 {
                match results[0] {
                    Value::I32(value) => Some(value as i64),
                    Value::I64(value) => Some(value),
                    _ => None,
                }
            } else {
                None
            }
        }

        None => {
            error!("Instance with ID `{}` isn't found.", instance_id);

            None
        }
    }
}

#[pg_extern]
fn invoke_function_0(instance_id: String, function_name: String) -> Option<i64> {
    invoke_function(instance_id, function_name, &[])
}

#[pg_extern]
fn invoke_function_1(instance_id: String, function_name: String, argument0: i64) -> Option<i64> {
    invoke_function(instance_id, function_name, &[argument0])
}

#[pg_extern]
fn invoke_function_2(
    instance_id: String,
    function_name: String,
    argument0: i64,
    argument1: i64,
) -> Option<i64> {
    invoke_function(instance_id, function_name, &[argument0, argument1])
}

#[pg_extern]
fn invoke_function_3(
    instance_id: String,
    function_name: String,
    argument0: i64,
    argument1: i64,
    argument2: i64,
) -> Option<i64> {
    invoke_function(
        instance_id,
        function_name,
        &[argument0, argument1, argument2],
    )
}

#[pg_extern]
fn invoke_function_4(
    instance_id: String,
    function_name: String,
    argument0: i64,
    argument1: i64,
    argument2: i64,
    argument3: i64,
) -> Option<i64> {
    invoke_function(
        instance_id,
        function_name,
        &[argument0, argument1, argument2, argument3],
    )
}

#[pg_extern]
fn invoke_function_5(
    instance_id: String,
    function_name: String,
    argument0: i64,
    argument1: i64,
    argument2: i64,
    argument3: i64,
    argument4: i64,
) -> Option<i64> {
    invoke_function(
        instance_id,
        function_name,
        &[argument0, argument1, argument2, argument3, argument4],
    )
}

#[pg_extern]
fn invoke_function_6(
    instance_id: String,
    function_name: String,
    argument0: i64,
    argument1: i64,
    argument2: i64,
    argument3: i64,
    argument4: i64,
    argument5: i64,
) -> Option<i64> {
    invoke_function(
        instance_id,
        function_name,
        &[
            argument0, argument1, argument2, argument3, argument4, argument5,
        ],
    )
}

#[pg_extern]
fn invoke_function_7(
    instance_id: String,
    function_name: String,
    argument0: i64,
    argument1: i64,
    argument2: i64,
    argument3: i64,
    argument4: i64,
    argument5: i64,
    argument6: i64,
) -> Option<i64> {
    invoke_function(
        instance_id,
        function_name,
        &[
            argument0, argument1, argument2, argument3, argument4, argument5, argument6,
        ],
    )
}

#[pg_extern]
fn invoke_function_8(
    instance_id: String,
    function_name: String,
    argument0: i64,
    argument1: i64,
    argument2: i64,
    argument3: i64,
    argument4: i64,
    argument5: i64,
    argument6: i64,
    argument7: i64,
) -> Option<i64> {
    invoke_function(
        instance_id,
        function_name,
        &[
            argument0, argument1, argument2, argument3, argument4, argument5, argument6, argument7,
        ],
    )
}

#[pg_extern]
fn invoke_function_9(
    instance_id: String,
    function_name: String,
    argument0: i64,
    argument1: i64,
    argument2: i64,
    argument3: i64,
    argument4: i64,
    argument5: i64,
    argument6: i64,
    argument7: i64,
    argument8: i64,
) -> Option<i64> {
    invoke_function(
        instance_id,
        function_name,
        &[
            argument0, argument1, argument2, argument3, argument4, argument5, argument6, argument7,
            argument8,
        ],
    )
}

#[pg_extern]
fn invoke_function_10(
    instance_id: String,
    function_name: String,
    argument0: i64,
    argument1: i64,
    argument2: i64,
    argument3: i64,
    argument4: i64,
    argument5: i64,
    argument6: i64,
    argument7: i64,
    argument8: i64,
    argument9: i64,
) -> Option<i64> {
    invoke_function(
        instance_id,
        function_name,
        &[
            argument0, argument1, argument2, argument3, argument4, argument5, argument6, argument7,
            argument8, argument9,
        ],
    )
}
