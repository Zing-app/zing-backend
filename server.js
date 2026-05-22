const express = require('express');
const app = express();
const http = require('http').createServer(app);

// Configuration Socket.io optimisée pour la production de masse
const io = require('socket.io')(http, {
    cors: { origin: "*" },
    pingTimeout: 3000,   // Si pas de réponse après 3s, l'appareil est déconnecté
    pingInterval: 10000, // Envoie un ping toutes les 10s pour maintenir le canal actif
    maxHttpBufferSize: 1e7 // Limite à 10 Mo par paquet binaire max
});

const crypto = require('crypto');
console.log(`\n🔥 BOOT ID: ${crypto.randomBytes(4).toString('hex').toUpperCase()}`);
console.log(`📁 REAL PATH: ${__filename}\n`);

const PORT = process.env.PORT || 3001;

// Bases de données temporaires en RAM (Globales)
let connectedUsers = new Map();
const activeTransfers = new Map(); // Stockage des morceaux de fichiers en cours de stream

// Algorithme d'Haversine de haute précision
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

    // 📍 RECEPTION & MATCHMAKING GPS AVEC CODE PIN DE SÉCURITÉ
    socket.on('update_location', (coords) => {
        if (!coords || !coords.latitude || !coords.longitude) return;

        let currentUser = connectedUsers.get(socket.id) || { pin: null, isVerified: false };

        connectedUsers.set(socket.id, {
            lat: coords.latitude,
            lng: coords.longitude,
            updatedAt: Date.now(),
            pin: currentUser.pin,
            isVerified: currentUser.isVerified
        });

        // Boucle de détection de proximité
        for (let [otherId, otherUser] of connectedUsers.entries()) {
            if (otherId === socket.id) continue;

            const distance = getDistance(coords.latitude, coords.longitude, otherUser.lat, otherUser.lng);
            const roomName = `room_${[socket.id, otherId].sort().join('_')}`;

            if (distance <= 80) {
                let myCurrentState = connectedUsers.get(socket.id);

                if (myCurrentState && !myCurrentState.pin && !otherUser.pin) {
                    const sharedPin = Math.floor(1000 + Math.random() * 9000).toString();

                    myCurrentState.pin = sharedPin;
                    otherUser.pin = sharedPin;

                    io.to(socket.id).emit('pin_generated', { pin: sharedPin, distance: distance.toFixed(1) });
                    io.to(otherId).emit('pin_generated', { pin: sharedPin, distance: distance.toFixed(1) });
                    console.log(`🔐 [PIN] Code ${sharedPin} généré pour la proximité.`);
                }
            } else {
                // Hors de portée -> Nettoyage de la room éphémère
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

    // 📤 ROUTAGE DES FLUX CLASSIQUES (Texte brut rapide)
    socket.on('send_to_room', (packageData) => {
        if (!packageData?.roomId || !packageData?.message) return;
        console.log(`📤 [PROPULSION] Flux type [${packageData.message.type}] envoyé dans la room : ${packageData.roomId}`);
        socket.to(packageData.roomId).emit('receive_message', packageData.message);
    });

    // 📦 RECEPTION FLUX BASE64 PAR PIPELINE CONTINU
    socket.on('send_chunk', (data) => {
        const { fileId, chunkIndex, chunkData, totalChunks, roomId, name, mimeType } = data;

        if (!activeTransfers.has(fileId)) {
            activeTransfers.set(fileId, {
                roomId,
                name,
                mimeType,
                totalChunks,
                chunksReceived: 0,
                buffer: []
            });
        }

        const transfer = activeTransfers.get(fileId);
        if (!transfer) return;

        // Convertit la chaîne Base64 reçue en Buffer binaire Node.js
        transfer.buffer[chunkIndex] = Buffer.from(chunkData, 'base64');
        transfer.chunksReceived++;

        const progress = Math.round((transfer.chunksReceived / transfer.totalChunks) * 100);
        io.to(transfer.roomId).emit('transfer_progress', { fileId, progress });

        if (transfer.chunksReceived === transfer.totalChunks) {
            console.log(`🚀 [STREAM DONE] ${transfer.name} réassemblé (${transfer.totalChunks} chunks Base64).`);

            const finalContent = Buffer.concat(transfer.buffer);

            io.to(transfer.roomId).emit('receive_message', {
                type: 'file',
                name: transfer.name,
                mimeType: transfer.mimeType,
                content: finalContent
            });

            activeTransfers.delete(fileId);
        }
    });

    // ❌ NETTOYAGE STRICT DES SESSIONS À LA DÉCONNEXION
    socket.on('disconnect', () => {
        connectedUsers.delete(socket.id);
        console.log(`❌ [SERVEUR] Déconnexion propre -> ID: ${socket.id}`);
    });
});

// Nettoyage de sécurité secondaire toutes les 60 secondes pour évacuer les inactifs
setInterval(() => {
    const now = Date.now();
    for (let [id, user] of connectedUsers.entries()) {
        if (now - user.updatedAt > 30000) { // Pas de rafraîchissement GPS depuis 30 secondes
            connectedUsers.delete(id);
            console.log(`🧹 [NETTOYAGE] Session expirée supprimée en RAM : ${id}`);
        }
    }
}, 60000);

http.listen(PORT, () => {
    console.log(`🚀 Serveur de Production ZING en ligne sur le port ${PORT}`);
});