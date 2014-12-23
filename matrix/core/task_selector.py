__author__ = 'xiaotian.wu'

from logger import logger

class TaskSelector:
  def __init__(self, offers, tasks):
    self.offers = offers
    self.tasks = tasks

  def match(self):
    scheduled_task = []
    for task in tasks:
      for offer in offers:
        logger.debug("offer detail: %s" % offer)
        if task.constraint.host == offer.hostname:
          task.offer = offer
          offers.remove(offer)
          scheduled_task.add(task)
          break
    return scheduled_task
