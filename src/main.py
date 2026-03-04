import os

from apscheduler.schedulers.blocking import BlockingScheduler

from src.scripts.hello_world import hello_world

def task():
    hello_world()


scheduler = BlockingScheduler()

scheduler.add_job(task, 'cron', day=1, hour=0, minute=1)

print("Agendador iniciado...", flush=True)
scheduler.start()