__author__ = 'xiaotian.wu'

from logger import logger

class TaskSelector:
  def __init__(self, offers, tasks):
    self.offers = offers
    self.tasks = tasks

  def match(self):
    scheduled_task = []
    for task in self.tasks:
      for offer in self.offers:
        logger.debug("offer detail: %s" % offer)
        if task.constraint.host == offer.hostname:
          task.offer = offer
          self.offers.remove(offer)
          scheduled_task.append(task)
          break
    return scheduled_task
