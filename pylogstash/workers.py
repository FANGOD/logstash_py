from importlib import import_module
# sys
import os
import sys
import json
import signal
# third some
import yaml
from loguru import logger
# process
import threading
import subprocess
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
# time
import time
from datetime import datetime
# from chinese_calendar import is_workday
# etl
from kafka import KafkaConsumer
from pymongo import MongoClient
from bson.objectid import ObjectId
from pymongo.errors import BulkWriteError
from pymongo import DeleteOne, ReplaceOne, UpdateOne, InsertOne


logger.remove()
logger.add(sys.stderr, format='<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <green>{extra[pid]}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>')
logger = logger.bind(pid=os.getpid())
logger.info("=" * 30 + " start " + "=" * 30)
logger = logger.patch(lambda record: record.update(name="pylogstash"))


def cleanup_handler(signum, frame):
    logger.warning("Received SIGTERM signal. Performing cleanup...")
    raise KeyboardInterrupt

signal.signal(signal.SIGTERM, cleanup_handler)


def default_doc_haneler(doc):
    try:
        if "_id" in doc:
            _id = ObjectId(doc["_id"])
            # for update
            return ReplaceOne({"_id": _id}, doc, upsert=True)
        else:
            return InsertOne(doc)
    except KeyboardInterrupt:
        raise
    except:
        logger.exception(f"{json.dumps(_doc)}")


class Mongodb(object):

    def __init__(self, logger, display_pec, uri, database, collection, **kwargs):
        self.logger = logger

        handler_file = kwargs.pop("script", "").replace("/", ".").split(".py")[0]
        if handler_file:
            self.doc_handler = import_module(handler_file).get_doc
        else:
            self.doc_handler = default_doc_haneler

        client = MongoClient(uri, **kwargs)
        db = client[database]
        self.collection = db[collection]
        self.test()
        self.display_cnt = 0
        self.display_pec = display_pec

    def bulk(self, bulks, exit=False, check=None, **kwargs):
        try:
            _from = "bulk"
            _deal_t = time.time()
            _bulk = []
            for data in bulks:
                data = self.doc_handler(data)
                if data:
                    _bulk.append(data)
            deal_t = time.time() - _deal_t

            _write_t = time.time()
            res = self.collection.bulk_write(_bulk, ordered=False)
            bulk_api_result = res.bulk_api_result
            bulk_api_result["upserted"] = len(bulk_api_result["upserted"])
            write_t = time.time() - _write_t

            self.display_cnt += 1
            if self.display_cnt == self.display_pec or exit or check == "time":
                if exit:
                    _from = "Cleanup"
                else:
                    _from = f"display_{int(100/(self.display_pec))}% "
                self.display_cnt = 0
                self.logger.success(f"check_{check}:{len(bulks)} from:{_from} {bulk_api_result} RT:{kwargs['read_t']:.4f} DT:{deal_t:.4f} WT:{write_t:.4f}")

        except BulkWriteError as bwe:
            write_t = time.time() - _write_t
            bulk_api_result = bwe.details
            bulk_api_result["upserted"] = len(bulk_api_result["upserted"])
            if bwe._OperationFailure__code == 65:
                bulk_api_result["writeErrors"] = len(bulk_api_result["writeErrors"])
                self.logger.info(f"check_{check}:{len(bulks)} from:{_from} {bulk_api_result} RT:{kwargs['read_t']:.4f} DT:{deal_t:.4f} WT:{write_t:.4f}")
            else:
                self.logger.exception("Uncaught exc: ")
        except KeyboardInterrupt:
            raise
        except:
            self.logger.exception("bulk error:")

    def test(self):
        try:
            results = list(self.collection.find_one())
            self.logger.info(results)
        except Exception as e:
            logger.warning(f"do test failed: {e}")


def kafka_reader(**kwargs):
    # procee's logger
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, format='<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <yellow>{extra[pid]}</yellow> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>')
    logger = logger.bind(pid=os.getpid())
    logger = logger.patch(lambda record: record.update(name="workers"))

    display_pec = kwargs["pipeline"].get("display.pec", 1)
    batch_size = kwargs["pipeline"].get("batch.size", 1000)
    if batch_size >= 10000:
        logger.error(f"Too big batch size to write subprocess, maybe lost data when stop")

    topics = kwargs["input"]["kafka"].pop("topics")
    assert isinstance(topics, list)

    consumer = KafkaConsumer(*topics, **kwargs["input"]["kafka"])
    writers = []

    for _output in kwargs["output"]:
        for out_type in _output:
            if out_type == "mongodb":
                writer_handler = Mongodb(logger, display_pec, **kwargs.get("filter", {"python": {}})["python"], **_output[out_type])
                writers.append(writer_handler)

    if not writers:
        msg = f"Not found writers, maybe the output configuration is wrong"
        logger.error(msg)
        raise KeyboardInterrupt(msg)

    bulks = []
    start_t = time.time()
    timer = None
    exit = False

    def process_bulks(check="bulk"):
        nonlocal bulks, start_t, exit, timer
        if timer:
            timer.cancel()
        timer = None

        read_t = time.time() - start_t
        for writer in writers:
            writer.bulk(bulks, read_t=read_t, exit=exit, check=check)

        bulks = []
        start_t = time.time()

    try:
        for msg in consumer:
            try:
                data = json.loads(msg.value.decode('utf-8'))
                bulks.append(data)
            except KeyboardInterrupt:
                raise
            except:
                logger.exception(f"Load kafka data to json error: {msg}")

            if len(bulks) >= batch_size:
                process_bulks()
            elif timer is None:
                timer = threading.Timer(60, process_bulks, args=("time",))
                timer.start()
    except KeyboardInterrupt:
        if bulks:
            exit = True
            logger.info(f"User KeyboardInterrupt, wait for cleanup subprocess data...")
            process_bulks()


def worker(name, **kwargs):
    workers = kwargs["pipeline"].get("workers", 2)
    _input = list(kwargs["input"].keys())
    _filter = list(kwargs.get("filter", {}).keys())
    _output = sum([list(i.keys()) for i in kwargs["output"]], [])
    logger.success(f"Run {workers} workers pipeline: {name} input:{_input} filter:{_filter} output:{_output}")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for i in range(workers):
            job = executor.submit(kafka_reader, **kwargs)
            futures[job] = i

        try:
            for future in as_completed(futures):
                try:
                    res = future.result()
                    seq_id = futures[future]
                    logger.info(f'kafka_reader({seq_id}) result is {res}')
                except ModuleNotFoundError as e:
                    raise
                except Exception as e:
                    logger.exception("as_completed exc:")
        except KeyboardInterrupt:
            wait_t = kwargs["pipeline"].get("batch.size", 1000) * 8 / 10000
            if wait_t < 0.5:
                wait_t = 0.5
            logger.info(f"Wait {wait_t}s for cleanup!")
            time.sleep(wait_t)
            logger.info(f"pipeline: {name} stopped!")
            sys.exit(0)


class ProcessManager:
    def __init__(self, config_path, debug=False, watch_interval=None):
        self.config_path = config_path
        self.debug = debug
        self.watch_interval = watch_interval
        self.processes = {}
        self.read_config()
        self.start_processes()
        self.message = None

    def read_config(self):
        with open(self.config_path, "r") as config_file:
            self.config = yaml.safe_load(config_file)
            if self.config is None:
                self.config = {}
            logger.success(f"Load config: {self.config_path}")
            if self.debug:
                logger.debug(f"config: {json.dumps(config, indent=4)}")

    def start_processes(self):
        if self.config:
            for process_name, process_config in self.config.items():
                self.start_process(process_name, process_config)

    def start_process(self, name, config):
        process = multiprocessing.Process(target=self.run_process, args=(name, config))
        process.start()
        self.processes[name] = process

    def run_process(self, name, config):
        logger.success(f"Starting pipeline: {name} with config: {list(config.keys())}")
        retry = 2
        while True:
            try:
                worker(name, **config)
            except (KeyboardInterrupt, SystemExit):
                raise
            except ModuleNotFoundError as e:
                logger.error(f"ModuleNotFoundError: {e}")
                logger.error(f"Remove error pipeline: {name}")
                self.remove_process(name)
                break
            except:
                logger.exception("error: run with config")
                retry -= 1
                if retry == 0:
                    raise

    def terminate_process_and_children(self, pid):
        try:
            command = f"pgrep -P {pid}"
            result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, text=True)
            process_ids = [int(i) for i in result.stdout.strip().split("\n") if i]

            logger.info(f"Found subprcess id: {process_ids}")
            for process_id in process_ids:
                logger.info(f"Send KeyboardInterrupt to subprocess: {process_id}")
                os.kill(process_id, 2)

            logger.info(f"Send KeyboardInterrupt to main: {pid}")
            os.kill(pid, 2)
        except ProcessLookupError as e:
            logger.warning(f"Not found process: {e}")
        except Exception as e:
            logger.exception(f"An error occurred: {e}")

    def restart_process(self, name, config):
        process = self.processes.get(name)
        if process:
            logger.success(f"Restarting pipeline {name} pid:{process.pid}....")
            self.terminate_process_and_children(process.pid)
            # os.killpg(os.getpgid(process.pid), 2)
            time.sleep(2)
            self.start_process(name, config)

    def remove_process(self, name):
        process = self.processes.pop(name, None)
        if process:
            logger.success(f"Removing pipeline: {name} pid:{process.pid}...")
            # os.killpg(os.getpgid(process.pid), 2)
            self.terminate_process_and_children(process.pid)

    def handle_config_change(self):
        new_config = yaml.safe_load(open(self.config_path, "r"))
        if new_config and new_config != self.config:
            for process_name in set(new_config.keys()) - set(self.config.keys()):
                self.start_process(process_name, new_config[process_name])
            for process_name in set(self.config.keys()) - set(new_config.keys()):
                self.remove_process(process_name)
            for process_name in set(new_config.keys()).intersection(set(self.config.keys())):
                if new_config[process_name] != self.config[process_name]:
                    self.restart_process(process_name, new_config[process_name])
            self.config = new_config
            return "Running..."
        elif new_config is None:
            if self.processes:
                self.exit_process()
            self.config = {}
            return "No config..."

    def exit_process(self):
        for process_name, process in self.processes.copy().items():
            self.remove_process(process_name)

    def auto_sleep(self):
        current_time = datetime.now()
        current_hour = current_time.hour
        # if is_workday(current_time) and 10 <= current_hour <= 19:
        #     self.sleep_interval = 5
        # else:
        #     self.sleep_interval = 3600
        time.sleep(self.watch_interval)

    def start(self):
        cnt = 0
        while True:
            cnt += 1
            display_interval = 3600
            message = self.handle_config_change()
            self.auto_sleep()
            if message and message != self.message:
                logger.info(f"watch interval: {self.watch_interval}s; display interval: {display_interval}s; watch result: {message}")
                self.message = message
            elif cnt == display_interval / 5:
                # The number of times at intervals of 5s
                logger.info(f"watch interval: {self.watch_interval}s; display interval: {display_interval}s; watch result: {message}")
                cnt = 0


def main(config_path, debug, watch_interval):
    try:
        manager = ProcessManager(config_path, debug, watch_interval)
        manager.start()
    except KeyboardInterrupt:
        logger.warning(f"pylogstash send exit to all pipelines")
        time.sleep(6)
        manager.exit_process()
        logger.success(f"pylogstash exit!")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="config.yaml")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--watch_interval", type=int, default=5)
    args = parser.parse_args()

    config_path = args.config
    debug = args.debug
    watch_interval = args.watch_interval
    main(config_path, debug, watch_interval)
