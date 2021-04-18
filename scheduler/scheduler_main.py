from datetime import datetime, timedelta
from time import sleep
from flask import Flask, jsonify, request
import threading
import heapq
import json
import requests
import socket

'''
    Start Service Deployment Route : /startdeployment
    End Service Deployment Route : /stopdeployment
'''

POLL_DURATION = 10000000 # can replace it with while True

WEEK_DAYS = {
    "Monday" : 0,
    "Tuesday" : 1,
    "Wednesday" : 2,
    "Thursday" : 3,
    "Friday" : 4,
    "Saturday" : 5,
    "Sunday" : 6
}


class Scheduler:
    def __init__(self):
        self.scheduling_queue = list()
        self.running_queue = list()
        self.id = 0
    
    def poll_scheduling_queue(self):
        curr_time = datetime.now()
        to_be_returned = list()
        while len(self.scheduling_queue) > 0:
            key, _id, request = self.scheduling_queue[0]
            if key <= curr_time:
                to_be_returned.append(request["value"])
                heapq.heappop(self.scheduling_queue)
                if request["repeating"]:
                    new_start_time = request["start_time"] + timedelta(days=7) #schedule it to next week
                    new_end_time = request["end_time"] + timedelta(days=7) #schedule it to next week
                    new_req = request.copy()
                    new_req["start_time"] = new_start_time
                    new_req["end_time"] = new_end_time
                    self.add_request(new_req, old_id=_id)
                #add to running queue
                self.add_to_running_queue(request, start_id=_id)
            else:
                break
        return to_be_returned

    def poll_running_queue(self):
        curr_time = datetime.now()
        to_be_returned = list()
        while len(self.running_queue) > 0:
            key, _id, request = self.running_queue[0]
            if key <= curr_time:
                to_be_returned.append(request["value"])
                heapq.heappop(self.running_queue)
            else:
                break

        return to_be_returned

    def add_request(self, request, old_id = None):
        if old_id is None:
            _id = self.id
            self.id += 1
        else:
            _id = old_id
        print(request)
        elem = (request["start_time"], _id, request)
        heapq.heappush(self.scheduling_queue, elem)
        print("request added")

    def add_to_running_queue(self, request, start_id):
        elem = (request["end_time"], start_id, request)
        heapq.heappush(self.running_queue, elem)
        print("request added to running queue")


scheduler = None

def poll_scheduler_start(scheduler):
    for _ in range(POLL_DURATION):
        start_req_list = scheduler.poll_scheduling_queue()
        for req in start_req_list:
            print("Start Request :", req["user_id"], req["app_id"], req["service_name_to_run"])
            requests.post("http://deployer:5001/startdeployment", json=req)
        sleep(1)

def poll_scheduler_end(scheduler):
    for _ in range(POLL_DURATION):
        end_req_list = scheduler.poll_running_queue()
        for req in end_req_list:
            print("End Request :", req["user_id"], req["app_id"], req["service_name_to_run"])
            requests.post("http://deployer:5001/stopdeployment", json=req)
        sleep(1)


def change_weekdays_to_indices(days):
    return list(map(lambda x : WEEK_DAYS[x], days))

def manage_scheduled_request(scheduler, schedule, request, appname, algo_name):
    start_times = schedule["time"]["startTimes"]
    durations = schedule["time"]["durations"]
    days = change_weekdays_to_indices(schedule["days"])
    for (start_time, dur) in zip(start_times, durations):
        h, m, s = start_time.split(":")
        h, m, s = int(h), int(m), int(s)
        today = datetime.today()
        start_time_obj = datetime(hour=h, minute=m, second=s,
                                    year=today.year, month=today.month, day=today.day)
        h, m, s = dur.split(":")
        h, m, s = int(h), int(m), int(s)
        duration = timedelta(hours=h, minutes=m, seconds=s)
        curr_weekday = datetime.today().weekday()
        for day in days:
            if day < curr_weekday:
                start_dtime_obj = start_time_obj + timedelta(days=7+day-curr_weekday)
            elif day > curr_weekday:
                start_dtime_obj = start_time_obj + timedelta(days=day-curr_weekday)
            else:
                if datetime.now() < start_time_obj:
                    start_dtime_obj = start_time_obj 
                else:
                    start_dtime_obj = start_time_obj + timedelta(days=7)
            end_time_obj = start_dtime_obj + duration
            request["service_id"] = str(scheduler.id)
            schedule_req = {
                "start_time" : start_dtime_obj,
                "end_time" : end_time_obj,
                "repeating" : True,
                "app_name" : appname,
                "algo_name" : algo_name,
                "value" : request
            }
            print("Scheduling : ")
            print(f"Start time : {start_dtime_obj}")
            print(f"End time : {end_time_obj}")
            scheduler.add_request(schedule_req)


def manage_immediate_request(scheduler, schedule, request, appname, algo_name):
    '''
    NOTE : for immediate requests we will still need the end time because start time will be
    immediate. So make sure it's filled in the JSON and it's only a one time schedule.
    NOTE : Length of durations list should be 1.
    '''
    start_time = datetime.now()
    request["service_id"] = str(scheduler.id)
    h, m, s = schedule["time"]["durations"][0].split(":")
    h, m, s = int(h), int(m), int(s)
    duration = timedelta(hours=h, minutes=m, seconds=s)
    end_time = start_time + duration

    schedule_req = {
        "start_time" : start_time,
        "end_time" : end_time,
        "repeating" : False,
        "app_name" : appname,
        "algo_name" : algo_name,
        "value" : request
    }
    print("Scheduling : ")
    print(f"Start time : {start_time}")
    print(f"End time : {end_time}")
    scheduler.add_request(schedule_req)

def manage_request(request):
    global scheduler
    for appname in request:
        for algo_name in request[appname]["algorithms"]:
            sensors = request[appname]["algorithms"][algo_name]["sensors"]
            new_request = {
                "user_id" : request[appname]["user_id"],
                "app_id" : request[appname]["application_name"],
                "service_name_to_run" : algo_name,
                "service_id" : None, #service_id
                "sensors" : sensors,
                "action" : request[appname]["algorithms"][algo_name]["action"]
            }
            schedule = request[appname]["algorithms"][algo_name]["schedule"]
            if request[appname]["algorithms"][algo_name]["isScheduled"]:
                manage_scheduled_request(scheduler, schedule, new_request, appname, algo_name)
            else:
                manage_immediate_request(scheduler, schedule, new_request, appname, algo_name)


app = Flask(__name__)

@app.route("/schedule_request", methods=["POST"])
def register_appliction():
    user_req = request.json
    manage_request(user_req)
    return jsonify({"msg" : "ok"})

if __name__ == '__main__':
    scheduler = Scheduler()
    start_poller = threading.Thread(target = poll_scheduler_start, args=(scheduler, ))
    end_poller = threading.Thread(target = poll_scheduler_end, args=(scheduler, ))
    start_poller.start()
    end_poller.start()

    app.run(debug=True, port=13337, host=socket.gethostbyname(socket.gethostname()))
