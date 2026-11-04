let {
    setActiveStatus,
    setLatitude,
    setLongitude,
    setSpeed,
    setColor,
} = require('./obu.js');
const mqtt = require('mqtt')

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });

// setSpeed(number) : Use this function to set the vehicle's speed (km/hr), e.g. setSpeed(50)
// setActiveStatus(boolean) : Use this function to set the vehicle's online status (true=Active, false=Inactive), e.g. setActiveStatus(true)
// setLatitude(number), setLongitude(number) : Use these functions to set the vehicle's location
    // e.g. setLatitude(13.738044), setLongitude(100.529944)
// setColor(string) : Use this function to set the vehicle-pin's color, e.g. setColor("blue")
    // valid colors are "blue", "green", "red", "violet", "yellow", "no"

// ======================= Subscriber =======================
let BROKER = (process.env.MQTT_BROKER || 'mqtt://localhost:1883').trim().replace(/^["']|["']$/g, '');
if (!/^[a-z]+:\/\//i.test(BROKER)) BROKER = 'mqtt://' + BROKER;
const V2X_TOPIC = process.env.V2X_TOPIC || '<your_v2x_topic>';

// The course server publishes under "IotMsgPlatformPartTwo/<v2x_topic>/...",
// while your own Sensor publisher uses "<v2x_topic>/...". Subscribe to both.
const TOPICS = [`${V2X_TOPIC}/#`, `IotMsgPlatformPartTwo/${V2X_TOPIC}/#`];

// Route filter: the course stream interleaves two tracks ~150 m apart, which makes the car teleport.
// Ignore points farther than MAX_JUMP_M from the last accepted one, and re-lock onto
// the next point after MAX_REJECTS rejections in a row (e.g. when the route loops back to its start).
const MAX_JUMP_M = 60;
const MAX_REJECTS = 5;
let lastPos = null;
let rejects = 0;

function distanceM(a, b) {
    const R = 6371000;
    const toRad = (d) => d * Math.PI / 180;
    const x = toRad(b.longitude - a.longitude) * Math.cos(toRad((a.latitude + b.latitude) / 2));
    const y = toRad(b.latitude - a.latitude);
    return Math.sqrt(x * x + y * y) * R;
}

function acceptRoute(pos) {
    if (!lastPos || distanceM(lastPos, pos) <= MAX_JUMP_M || rejects >= MAX_REJECTS) {
        lastPos = pos;
        rejects = 0;
        return true;
    }
    rejects++;
    return false;
}

const client = mqtt.connect(BROKER, {
    clientId: 'obu_consumer_' + Math.random().toString(16).slice(3),
    clean: true,
    reconnectPeriod: 1000,
});

client.on('connect', () => {
    console.log(`Connected to MQTT broker ${BROKER}`);
    client.subscribe(TOPICS, (err) => {
        if (err) console.error('Subscribe failed:', err);
        else console.log(`Subscribed to ${TOPICS.join(', ')}`);
    });
});

client.on('message', (topic, message) => {
    let data;
    try {
        data = JSON.parse(message.toString());
    } catch (err) {
        console.error(`Invalid JSON on ${topic}: ${message.toString()}`);
        return;
    }

    const subtopic = topic.split('/').pop();
    switch (subtopic) {
        case 'speed':
            setSpeed(Number(data.speed));
            break;
        case 'heartbeat': {
            // Accept true/false (own publisher) and "ACTIVE"/"INACTIVE" strings
            const hb = data.heartbeat;
            setActiveStatus(hb === true || hb === 'true' || hb === 'ACTIVE');
            break;
        }
        case 'route': {
            const pos = { latitude: Number(data.latitude), longitude: Number(data.longitude) };
            if (!acceptRoute(pos)) return;
            setLatitude(pos.latitude);
            setLongitude(pos.longitude);
            setColor(data.color || 'no');
            break;
        }
        default:
            return;
    }
    console.log(`[${topic}] ${message.toString()}`);
});

client.on('error', (err) => console.error('MQTT error:', err.code || err));
client.on('offline', () => console.log('MQTT client is offline'));