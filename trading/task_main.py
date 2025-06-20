from database.sqlite import SQLiteDB

class TaskManager:
    def __init__(self):
        self.db = SQLiteDB()

    def add_task(self, task_name, task_params):
        result = self.db.create_task(task_name, task_params)
        return result
    def get_tasks(self):
        return self.db.get_tasks()

    def delete_task(self, task_id):
        self.db.delete_task(task_id)