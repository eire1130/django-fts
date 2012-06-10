from django.conf import settings

FTS_BACKEND = getattr(settings, 'FTS_BACKEND', 'simple://')
FTS_CONFIGURE_ALL_BACKENDS = getattr(settings, 'FTS_CONFIGURE_ALL_BACKENDS', False)

FTS_DATABASE = getattr(settings, 'FTS_DATABASE', 'default')
