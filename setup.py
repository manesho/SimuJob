from setuptools import setup

setup(name='simujob',
      version='1.0',
      description='A lightweight tool for simulations on a grid engine',
      url='https://github.com/manesho/SimuJob',
      author='Manes Hornung',
      author_email='manes.hornung@gmail.com',
      packages=['simujob'],
      install_requies=[
          'xarray','numpy','pandas'
      ],
      zip_safe=False)

