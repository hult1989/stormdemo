from fabric.api import *

env.hosts = ['ubuntu@configserver']
env.key_filename = '~/hermes'
#env.hosts = ['tangh@server']
#env.key_filename = '~/server'

def sync():
    local('mvn assembly:assembly')
    put('./target/stormdemo-1.0-jar-with-dependencies.jar', '~/apache-storm-1.0.2/')
    put('./storm.config', '~/apache-storm-1.0.2/')


def calc():
    with cd('~/apache-storm-1.0.2'):
        run("grep '\tevent\t' matcher.out | awk '{print $5}'> event.rtt")
        run("grep '\tevent\t' matcher.out | awk '{print $4}'> event.size")
        run("grep '\tsub\t' matcher.out | awk '{print $5}'> sub.rtt")
        run("grep '\tevent\t' matcher.out | awk '{print $1}'| head -n 1 > event.time")
        run("grep '\tevent\t' matcher.out | awk '{print $1}'| tail -n 1 >> event.time")
        run("grep '\tsub\t' matcher.out | awk '{print $1}'| head -n 1 > sub.time")
        run("grep '\tsub\t' matcher.out | awk '{print $1}'| tail -n 1 >> sub.time")
        run('python calc.py')

def rsync():
    local('mvn assembly:assembly')
    put('./target/stormdemo-1.0-jar-with-dependencies.jar', '~/storm/matcher.jar')
    put('./storm.config', '~/storm/storm.config')
    put('./calc.py', '~/storm/calc.py')

