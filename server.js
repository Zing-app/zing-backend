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

    // 📍 RECEPTION & MATCHMAKING GPS
    socket.on('update_location', (coords) => {
        if (!coords?.latitude || !coords?.longitude) return;

        // Mise à jour ou création de l'utilisateur dans la Map
        connectedUsers.set(socket.id, { lat: coords.latitude, lng: coords.longitude, updatedAt: Date.now() });
        console.log(`📍 [GPS] ID ${socket.id.substring(0, 5)}... : [${coords.latitude.toFixed(4)}, ${coords.longitude.toFixed(4)}]`);

        // Boucle de tracking de proximité croisée
        for (let [otherId, otherUser] of connectedUsers.entries()) {
            if (otherId === socket.id) continue;

            const distance = getDistance(coords.latitude, coords.longitude, otherUser.lat, otherUser.lng);
            const roomName = `room_${[socket.id, otherId].sort().join('_')}`;

            if (distance <= 80) { // Rayon de tolérance de 80m validé en intérieur 🏠
                socket.join(roomName);
                io.sockets.sockets.get(otherId)?.join(roomName);

                io.to(roomName).emit('near_user_found', { roomId: roomName, distance: distance.toFixed(1) });
                console.log(`👥 [MATCH] Proximité détectée (${distance.toFixed(1)}m) entre ${socket.id.substring(0, 5)} et ${otherId.substring(0, 5)}`);
            } else {
                // Rupture de portée
                if (socket.rooms.has(roomName)) {
                    socket.leave(roomName);
                    io.sockets.sockets.get(otherId)?.leave(roomName);
                    io.to(socket.id).emit('room_lost');
                    io.to(otherId).emit('room_lost');
                    console.log(`🏃‍♂️ [PORTÉE] Écart trop grand (${distance.toFixed(1)}m). Salon détruit.`);
                }
            }
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