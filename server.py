#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Harvest IP addresses from torrents'''

import re
import os
import sys
import pickle
import time
import urllib
import random
import argparse

import pymongo
import libtorrent as libtorrent


class MagnetDaemon:

    def __init__(self, config):
        self.config = config
        self.session = libtorrent.session()
        # self.session.listen_on(8888, 9999)
        self.session.start_dht()
        # self.session.stop_upnp()
        # self.session.stop_natpmp()

        self.session.set_download_rate_limit(10000)
        self.session.set_max_connections(1000)

        self.mongconn = pymongo.Connection()
        self.db = self.mongconn['magnet']
        self.peers = self.db.peers
        self.hashes = self.db.hashes
        self.handle_ages = {}

    def create_handle(self, link=None, path=None):
        params = {
            'save_path': 'tmp',
            # 'upload_mode': True,
            'storage_mode': libtorrent.storage_mode_t.storage_mode_compact,
            'paused': False,
            'auto_managed': False,  # Disables queueing
        }
        if link:
            h = libtorrent.add_magnet_uri(self.session, link, params)
        if path:
            h = libtorrent.add_files(self.session, path, params)

        return h

    def create_magnet_url(self, info_hash):
        trackers = [
            'http://tracker.openbittorent.org/announce',
            'http://tracker.publicbt.org/announce',
        ]

        s = 'magnet:?xt=urn:btih:%s' % info_hash
        for t in trackers:
            s += '&tr='
            s += urllib.quote(t)
        return str(s)

    def add_from_hash(self, info_hash):
        link = self.create_magnet_url(info_hash)
        handle = self.create_handle(link=link)

    def hash_from_magnet(self, uri):
        m = re.search('btih:([^&]*)&', uri)
        return m.group(1)

    def load_hashes_from_pickle(self, path):
        d = pickle.load(open(path, 'rb'))
        for item in d:
            info_hash = self.hash_from_magnet(item['downloadLink'])
            d = {'hash': info_hash}
            dbhash = self.hashes.find_one(d)
            if not dbhash:
                self.hashes.insert(d)

    def load_hashes_from_dump(self, path):
        no = 0
        for line in open(path):
            no += 1
            if no % 10000 == 0:
                print no
            line = line.strip()
            info_hash = self.hash_from_line(line)
            d = {'hash': str(info_hash)}
            dbhash = self.hashes.find_one(d)
            if not dbhash:
                self.hashes.insert(d)
        print 'Done'

    def hash_from_line(self, line):
        h = line.split('|')
        return h[-1]

    def reset_hashes(self):
        '''Remove all hashes from the hashes collection in mongodb'''
        self.hashes.remove()

    def remove(self, handle):
        del self.handle_ages[str(handle.info_hash())]
        self.session.remove_torrent(handle)

    def monitor(self):
        for torrent in self.session.get_torrents():

            print 'C %-04i' % len(torrent.get_peer_info()),

            print 'S %-04i /' % torrent.status().num_seeds,
            print '%-04i' % torrent.status().list_seeds,

            print 'P %-04i /' % torrent.status().num_peers,
            print '%-04i' % torrent.status().list_peers,

            print torrent.name(),
            print

            info_hash = str(torrent.info_hash())
            if info_hash not in self.handle_ages:
                self.handle_ages[info_hash] = time.time()

            dt = time.time() - self.handle_ages[info_hash]
            if dt > self.config.torrent_time:
                self.remove(torrent)
                print ' -> REMOVED'
                continue

            self.add_peers_to_db(torrent)

        print 80 * '-'

    def run(self):
        while 1:
            self.add_random_torrents(self.config.max_torrents)
            self.monitor()
            time.sleep(self.config.torrent_sleep)

    def add_random_torrents(self, limit):
        hashes = None
        while len(self.session.get_torrents()) < limit:

            # XXX: probably memory/time hog
            if not hashes:
                hashes = list(self.hashes.find())

            c = random.choice(hashes)
            self.add_from_hash(c['hash'])

    def add_peers_to_db(self, torrent):
        peers = torrent.get_peer_info()

        for p in peers:
            # print ' - %s %s' % (p.ip[0], p.client)

            addr = p.ip[0]
            peer = self.peers.find_one({'addr': addr})
            if not peer:
                peer = {
                    'addr': addr,
                    'torrents': []
                    }

            peer['client'] = unicode(p.client, 'utf-8')
            ih = str(torrent.info_hash())
            if ih not in peer['torrents']:
                peer['torrents'].append(ih)
            self.peers.save(peer)


class Main:

    def run(self):
        config = self.parse_cmdline()
        md = MagnetDaemon(config)

        quit = False

        if config.reset_hashes:
            print 'Removing all hashes...'
            md.reset_hashes()
            quit = True

        if config.tpb_dump:
            print 'Importing tpb dump from', config.tcp_dump
            md.load_hashes_from_dump(config.tpb_dump)
            quit = True

        if config.pickle_dump:
            print 'Importing pickle dump from', config.pickle_dump
            md.load_hashes_from_pickle(config.pickle_dump)
            quit = True

        if config.hash:
            md.add_from_hash(config.hash)
            quit = True

        if config.dump_hashes:
            for a in md.hashes.find():
                print a['hash']
            quit = True

        if config.dump_peers:
            for a in md.peers.find():
                print a
                print u'%s,%s' % (a['addr'], a['client'].decode('utf-8')),
                print u','.join(a['torrents'])
            quit = True

        if quit and not config.dont_quit:
            return

        md.run()

    def parse_cmdline(self):
        parser = argparse.ArgumentParser(description=__doc__)

        parser.add_argument('-r', dest='reset_hashes', action='store_true',
            help='remove all imported hashes from mongodb')

        parser.add_argument('-tpb', dest='tpb_dump',
            help='import hashes from the pirate bay dump')
        parser.add_argument('-p', dest='pickle_dump',
            help='import hashes from a pickle dump')
        parser.add_argument('-s', dest='hash', help='import a single hash')

        parser.add_argument('-dh', dest='dump_hashes', action='store_true',
            help='dump all hashes')
        parser.add_argument('-dp', dest='dump_peers', action='store_true',
            help='dump all peers')

        parser.add_argument('-ct', dest='max_torrents', help='limit to'
            ' this number of torrents at a time', default=100, type=int)
        parser.add_argument('-tt', dest='torrent_time', help='monitor'
            ' each torrent for this amount of time', default=10, type=float)
        parser.add_argument('-ts', dest='torrent_sleep', help='time'
            ' between each polling of libtorrent', default=.1, type=float)

        parser.add_argument('-c', dest='dont_quit', action='store_true',
            help='dont quit after importing, maintenance, etc.')

        return parser.parse_args()


if __name__ == '__main__':
    Main().run()

