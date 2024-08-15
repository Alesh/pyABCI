import importlib.machinery
import importlib.util
import os.path


def load_module_from_file(filename):
    module_name = os.path.basename(filename).split('.')[0]
    loader = importlib.machinery.SourceFileLoader(module_name, filename)
    spec = importlib.util.spec_from_loader(module_name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def resolve_app(app_name: str):
    module_name, app_name = app_name.split(':')
    if module_name.endswith('.py'):
        module = load_module_from_file(module_name)
    else:
        module = importlib.import_module(module_name)
    return getattr(module, app_name)
