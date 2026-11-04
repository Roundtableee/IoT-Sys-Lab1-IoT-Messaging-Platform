const mqtt = require('mqtt')

// Load the configuration
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') }); // Sensor/server/.env

const Producer = () => {
    let client
    let BROKER = (process.env.MQTT_BROKER || 'mqtt://localhost:1883').trim().replace(/^["']|["']$/g, '');
    if (!/^[a-z]+:\/\//i.test(BROKER)) BROKER = 'mqtt://' + BROKER;
    console.log('Producer using broker:', BROKER);
    const options = {
        connectTimeout: 5000,
        clientId: 'nodejs_client_' + Math.random().toString(16).slice(3),
        clean: true,
        reconnectPeriod: 1000,
    };

    const connect = async() => {
        return new Promise((resolve, reject) => {
            client = mqtt.connect(BROKER, options);

            client.on('connect', () => {
                console.log('Connected to MQTT broker');
                resolve();
            });

            client.on('error', (error) => {
                console.error('MQTT connection error:', error);
                reject(error);
            });

            // Optional, just for logging/debugging
            client.on('offline', () => console.log('MQTT client is offline'));
            client.on('reconnect', () => console.log('Attempting to reconnect to MQTT broker'));
        });
    }

    const publish = async(topic, msg) => {
        return new Promise((resolve, reject) => {
            if (!client) {
                return reject(new Error("Producer is not connected."));
            }

            const options = {
                qos: 0,
                retain: false,
                dup: false
            }

            client.publish(topic, msg, options, (err) => {
                if (err) {
                    console.error(`Error publishing to topic ${topic}:`, err);
                    reject(err);
                } else {
                    console.log(`Successfully published to topic ${topic}`);
                    resolve();
                }
            });
        });
    }

    const close = async() => {
        return new Promise((resolve, reject) => {
            if (!client) {
                return reject(new Error("No connection to close."));
            }

            client.end(false, {}, (err) => {
                if (err) {
                    console.error(`Error closing connection:`, err);
                    reject(err);
                } else {
                    console.log(`Connection to ${BROKER} closed.`);
                    resolve();
                }
            });
        });
    }

    return {
        connect,
        publish,
        close
    }
}

module.exports = Producer;