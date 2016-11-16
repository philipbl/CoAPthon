from distutils.core import setup

setup(
    name='CoAPy',
    version='4.0.2',
    packages=['coapthon', 'coapthon.layers', 'coapthon.client', 'coapthon.server', 'coapthon.messages',
              'coapthon.forward_proxy', 'coapthon.resources', 'coapthon.reverse_proxy'],
    url='https://github.com/philipbl/CoAPy',
    license='MIT License',
    author='Philip Lundrigan',
    author_email='philipbl@cs.utah.edu',
    description='CoAPy is a python implementation for the CoAP protocol.',
    scripts=['coapserver.py', 'coapclient.py', 'exampleresources.py', 'coapforwardproxy.py', 'coapreverseproxy.py'],
    requires=['sphinx'],
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Development Status :: 4 - Beta",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
