from .coordinator_single import CoordinatorSingle
from .coordinator_celery import CoordinatorCelery


RUN_COORDINATORS = {}


def register_coordinator(variant):
    def decorator(fn):
        RUN_COORDINATORS[variant] = fn
        return fn

    return decorator


@register_coordinator('single')
def coordinator_single(conf):
    return CoordinatorSingle(task_group=conf)


@register_coordinator('celery')
def coordinator_celery(conf):
    return CoordinatorCelery(task_group=conf)


def get_coordinator(variant):
    if variant not in RUN_COORDINATORS:
        raise ValueError(f'Unsupported coordinator type: {variant}')
    return RUN_COORDINATORS.get(variant)
