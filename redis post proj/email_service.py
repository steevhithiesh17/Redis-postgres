import importlib.util
import os

_path = os.path.join(os.path.dirname(__file__), "email service.py")
spec = importlib.util.spec_from_file_location("email_service_impl", _path)
email_service_impl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(email_service_impl)
# Re-export public symbols
for _name in dir(email_service_impl):
    if not _name.startswith("_"):
        globals()[_name] = getattr(email_service_impl, _name)
