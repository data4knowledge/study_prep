import time
from locust import HttpUser, task, between

class my_user(HttpUser):
    @task
    def root(self):
        print("get root")
        self.client.get("/")

    @task
    def studies(self):
        print("get /studies")
        self.client.get("/studies/")

    @task
    def cdisc_pilot(self):
        print("get /cdisc_pilot")
        self.client.get("/cdisc_pilot/")

    @task
    def forms(self):
        print("get /cdisc_pilot")
        self.client.get("/studyDesigns/096047c3-8954-4a96-a075-3a750b929ea8/forms")
