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

    // 📍 RECEPTION GPS & REGROUPEMENT MULTI-UTILISATEURS (Version Cluster de Groupe)
    socket.on('update_location', (coords) => {
        if (!coords || !coords.latitude || !coords.longitude) return;

        let currentUser = connectedUsers.get(socket.id) || { pin: null, isVerified: false, roomId: null };

        // Mettre à jour la position de l'émetteur
        connectedUsers.set(socket.id, {
            lat: coords.latitude,
            lng: coords.longitude,
            updatedAt: Date.now(),
            pin: currentUser.pin,
            isVerified: currentUser.isVerified,
            roomId: currentUser.roomId
        });

        let sharedPinToUse = currentUser.pin;
        let roomIdToUse = currentUser.roomId;

        // 🔍 1. Parcourir les utilisateurs pour trouver si un groupe existe déjà à moins de 80m
        for (let [otherId, otherUser] of connectedUsers.entries()) {
            if (otherId === socket.id) continue;

            const distance = getDistance(coords.latitude, coords.longitude, otherUser.lat, otherUser.lng);

            if (distance <= 80) {
                // Si une personne proche a déjà un PIN et une Room, on s'accroche à son groupe !
                if (otherUser.pin && otherUser.roomId) {
                    sharedPinToUse = otherUser.pin;
                    roomIdToUse = otherUser.roomId;
                    break;
                }
            }
        }

        // 🔍 2. Si personne autour n'a de groupe, mais qu'il y a du monde, on crée le premier groupe
        if (!sharedPinToUse) {
            for (let [otherId, otherUser] of connectedUsers.entries()) {
                if (otherId === socket.id) continue;

                const distance = getDistance(coords.latitude, coords.longitude, otherUser.lat, otherUser.lng);
                if (distance <= 80) {
                    sharedPinToUse = Math.floor(1000 + Math.random() * 9000).toString(); // PIN unique du groupe
                    roomIdToUse = `group_room_${sharedPinToUse}`;
                    break;
                }
            }
        }

        // 🔍 3. Assigner le groupe à l'utilisateur actuel s'il a changé
        if (sharedPinToUse && currentUser.pin !== sharedPinToUse) {
            let myState = connectedUsers.get(socket.id);
            myState.pin = sharedPinToUse;
            myState.roomId = roomIdToUse;

            // On envoie le PIN de groupe au téléphone
            socket.emit('pin_generated', { pin: sharedPinToUse });
            console.log(`👥 [GROUPE] ID ${socket.id} a rejoint le groupe PIN: ${sharedPinToUse}`);
        }
    });

    // 🔓 VALIDATION DU PIN POUR TOUT LE GROUPE
    socket.on('verify_pin', (data) => {
        const user = connectedUsers.get(socket.id);
        
        if (user && user.pin === data.pin) {
            user.isVerified = true;
            socket.join(user.roomId); // Rejoint le salon de groupe Socket.io

            // On informe cet utilisateur spécifique que sa connexion est validée
            socket.emit('near_user_found', { roomId: user.roomId, distance: "Confirmée" });
            console.log(`🔓 [ACCESS] ID ${socket.id} validé dans le salon de groupe : ${user.roomId}`);
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