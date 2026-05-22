const express = require('express');
const app = express();
const http = require('http').createServer(app);

// Configuration Socket.io optimisée pour la production
const io = require('socket.io')(http, {
    cors: { origin: "*" },
    pingTimeout: 3000,   // Si pas de réponse après 3s, on considère l'appareil déconnecté
    pingInterval: 10000, // Envoie un ping toutes les 10s pour vérifier si l'app est ouverte
    maxHttpBufferSize: 1e7 // Augmente la limite à 10 Mo pour autoriser des photos plus lourdes 🖼️
});

const crypto = require('crypto');
console.log(`\n🔥 BOOT ID: ${crypto.randomBytes(4).toString('hex').toUpperCase()}`);
console.log(`📁 REAL PATH: ${__filename}\n`);

// Le port est récupéré dynamiquement si tu le déploies en ligne, sinon 3001
const PORT = process.env.PORT || 3001;

// Base de données temporaire en RAM
let connectedUsers = new Map();

// Algorithme d'Haversine optimisé
function getDistance(lat1, lon1, lat2, lon2) {
    const R = 6371000; // Rayon de la Terre en mètres
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a =
        Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
    return R * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
}

io.on('connection', (socket) => {
    console.log(`🔌 [SERVEUR] Connexion établie -> ID: ${socket.id}`);

    // 📍 RECEPTION & MATCHMAKING GPS AVEC CODE PIN DE SÉCURITÉ (Version Ultra-Stable)
    socket.on('update_location', (coords) => {
        if (!coords || !coords.latitude || !coords.longitude) return;

        // Récupération sécurisée de l'ancien état utilisateur pour éviter le ?.
        let currentUser = connectedUsers.get(socket.id) || { pin: null, isVerified: false };

        connectedUsers.set(socket.id, {
            lat: coords.latitude,
            lng: coords.longitude,
            updatedAt: Date.now(),
            pin: currentUser.pin,
            isVerified: currentUser.isVerified
        });

        // Boucle de vérification de proximité
        for (let [otherId, otherUser] of connectedUsers.entries()) {
            if (otherId === socket.id) continue;

            const distance = getDistance(coords.latitude, coords.longitude, otherUser.lat, otherUser.lng);
            const roomName = `room_${[socket.id, otherId].sort().join('_')}`;

            if (distance <= 80) {
                let myCurrentState = connectedUsers.get(socket.id);

                // Si aucun PIN n'est généré pour ce duo, on crée un code unique
                if (myCurrentState && !myCurrentState.pin && !otherUser.pin) {
                    const sharedPin = Math.floor(1000 + Math.random() * 9000).toString(); // Ex: "4732"

                    myCurrentState.pin = sharedPin;
                    otherUser.pin = sharedPin;

                    // On envoie le PIN aux deux appareils pour affichage
                    io.to(socket.id).emit('pin_generated', { pin: sharedPin, distance: distance.toFixed(1) });
                    io.to(otherId).emit('pin_generated', { pin: sharedPin, distance: distance.toFixed(1) });
                    console.log(`🔐 [PIN] Code ${sharedPin} généré pour la proximité.`);
                }
            } else {
                // Hors de portée -> Nettoyage strict
                if (socket.rooms.has(roomName)) {
                    socket.leave(roomName);
                    let targetSocket = io.sockets.sockets.get(otherId);
                    if (targetSocket) targetSocket.leave(roomName);

                    let myState = connectedUsers.get(socket.id);
                    if (myState) { myState.pin = null; myState.isVerified = false; }
                    otherUser.pin = null;
                    otherUser.isVerified = false;

                    io.to(socket.id).emit('room_lost');
                    io.to(otherId).emit('room_lost');
                }
            }
        }
    });

    // 🔓 ÉCOUTE DE LA VALIDATION DU PIN
    socket.on('verify_pin', (data) => {
        const user = connectedUsers.get(socket.id);
        if (user && user.pin === data.pin) {
            user.isVerified = true;

            // Trouver l'autre utilisateur pour ouvrir la room
            for (let [otherId, otherUser] of connectedUsers.entries()) {
                if (otherId !== socket.id && otherUser.pin === data.pin && otherUser.isVerified) {
                    const roomName = `room_${[socket.id, otherId].sort().join('_')}`;
                    socket.join(roomName);
                    io.sockets.sockets.get(otherId)?.join(roomName);

                    io.to(roomName).emit('near_user_found', { roomId: roomName, distance: "Confirmée" });
                    console.log(`🔓 [ACCESS] Code PIN validé. Room éphémère activée : ${roomName}`);
                }
            }
        } else {
            socket.emit('pin_error', { message: "Code PIN incorrect" });
        }
    });

    // 📤 ROUTAGE DES FLUX (Texte & Images Base64)
    socket.on('send_to_room', (packageData) => {
        if (!packageData?.roomId || !packageData?.message) return;

        console.log(`📤 [PROPULSION] Flux type [${packageData.message.type}] envoyé dans la room : ${packageData.roomId}`);
        // Émet à tout le monde dans le salon SAUF à l'émetteur
        socket.to(packageData.roomId).emit('receive_message', packageData.message);
    });

    // ❌ NETTOYAGE STRICT DES FANTÔMES
    socket.on('disconnect', () => {
        connectedUsers.delete(socket.id);
        console.log(`❌ [SERVEUR] Déconnexion propre -> ID: ${socket.id}`);
    });
});

// Nettoyage de sécurité secondaire (toutes les 60 secondes, on vire les sockets inactives)
setInterval(() => {
    const now = Date.now();
    for (let [id, user] of connectedUsers.entries()) {
        if (now - user.updatedAt > 30000) { // Pas de signal GPS depuis 30s
            connectedUsers.delete(id);
            console.log(`🧹 [NETTOYAGE] Session expirée supprimée en RAM : ${id}`);
        }
    }
}, 60000);

http.listen(PORT, () => {
    console.log(`🚀 Serveur de Production ZING en ligne sur le port ${PORT}`);
});