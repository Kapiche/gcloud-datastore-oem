from django.conf import settings
from django.contrib.auth.hashers import make_password
from django.contrib.auth.models import UserManager
from django.db import models
try:
    from django.utils.module_loading import import_module
except ImportError:
    """Handle older versions of Django"""
    from django.utils.importlib import import_module
from django.utils.translation import ugettext_lazy as _


__all__ = (
    'get_user_document',
)


GCLOUDOEM_USER_ENTITY = getattr(
    settings, 'GCLOUDOEM_USER_DOCUMENT', 'gcloudoem.django.auth.User')


def get_user_entity():
    """
    Get the user document class used for authentication.

    This is the class defined in settings.GCLOUDOEM_USER_DOCUMENT, which defaults to `gcloudoem.django.auth.User`.
    """

    name = GCLOUDOEM_USER_ENTITY
    dot = name.rindex('.')
    module = import_module(name[:dot])
    return getattr(module, name[dot + 1:])


class GCloudUserManager(UserManager):
    """
    A User manager which allows the use of GCloudOEM documents in Django.

    To use the manager, you must tell django.contrib.auth to use GCloudUser as the user model. In you settings.py, you
    need:

        INSTALLED_APPS = (
            ...
            'django.contrib.auth',
            'gcloudoem.django.gcloud_auth',
            ...
        )
        AUTH_USER_MODEL = 'gcloud_auth.GCloudUser'

    Django will use the model object to access the custom Manager, which will replace the original queryset with
    GCloudOEM querysets.

    By default, gcloudoem.django.auth.User will be used to store users. You can specify another entity class in
    GCLOUDOEM_USER_ENTITY in your settings.py.

    The User Entity class has the same requirements as a standard custom user model:
    https://docs.djangoproject.com/en/dev/topics/auth/customizing/

    In particular, the User Entity class must define USERNAME_FIELD and REQUIRED_FIELDS.

    `AUTH_USER_MODEL` has been added in Django 1.5.
    """

    def contribute_to_class(self, model, name):
        super(GCloudUserManager, self).contribute_to_class(model, name)
        self.dj_model = self.model
        self.model = get_user_entity()

        self.dj_model.USERNAME_FIELD = self.model.USERNAME_FIELD
        username = models.CharField(_('username'), max_length=30, unique=True)
        username.contribute_to_class(self.dj_model, self.dj_model.USERNAME_FIELD)

        self.dj_model.REQUIRED_FIELDS = self.model.REQUIRED_FIELDS
        for name in self.dj_model.REQUIRED_FIELDS:
            field = models.CharField(_(name), max_length=30)
            field.contribute_to_class(self.dj_model, name)

    def get(self, *args, **kwargs):
        try:
            return self.get_query_set().get(*args, **kwargs)
        except self.model.DoesNotExist:
            # ModelBackend expects this exception
            raise self.dj_model.DoesNotExist

    @property
    def db(self):
        raise NotImplementedError

    def get_empty_query_set(self):
        return self.model.objects.none()

    def get_query_set(self):
        return self.model.objects


class GCloudUser(models.Model):
    """"
    Dummy user model for GCloud Datastore.

    GCloudUser is used to replace Django's UserManager with MongoUserManager. The actual user document class is
    gcloudoem.django.auth.User or any other document class specified in GCLOUDOEM_USER_ENTITY.

    To get the user entity class, use `get_user_entity()`.
    """
    objects = GCloudUserManager()

    class Meta:
        app_label = 'gcloud_auth'

    def set_password(self, password):
        """Doesn't do anything, but works around the issue with Django 1.6."""
        make_password(password)
