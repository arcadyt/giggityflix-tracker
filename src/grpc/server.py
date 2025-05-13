import logging

import grpc

# These imports will be available once the protobuf files are compiled
# If you get import errors, run the protobuf generator first
from src.grpc.generated import tracker_pb2 as pb2
from src.grpc.generated import tracker_pb2_grpc as pb2_grpc
from src.services.tracker_service import TrackerService

logger = logging.getLogger(__name__)


class TrackerServicer(pb2_grpc.TrackerServiceServicer):
    """Implementation of the TrackerService gRPC service."""

    def __init__(self, tracker_service: TrackerService):
        self.tracker_service = tracker_service

    def GetPeersForCatalog(self, request, context):
        """Get peers that have a specific catalog_id."""
        try:
            catalog_id = request.catalog_id
            peers = self.tracker_service.get_peers_for_catalog(catalog_id)

            # Convert to protobuf response
            response = pb2.PeersResponse()
            for peer in peers:
                peer_info = pb2.PeerInfo(
                    peer_id=peer.peer_id,
                    edge_id=peer.edge_id,
                    connection_timestamp=int(peer.connected_at.timestamp()),
                    connection_quality=1.0  # Default quality metric
                )
                response.peers.append(peer_info)

            return response

        except Exception as e:
            logger.error(f"Error in GetPeersForCatalog: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return pb2.PeersResponse()

    def GetCatalogIdsForPeer(self, request, context):
        """Get all catalog_ids owned by a peer."""
        try:
            peer_id = request.peer_id
            catalog_ids = self.tracker_service.get_catalog_ids_for_peer(peer_id)

            # Convert to protobuf response
            response = pb2.CatalogIdsResponse(catalog_ids=catalog_ids)
            return response

        except Exception as e:
            logger.error(f"Error in GetCatalogIdsForPeer: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return pb2.CatalogIdsResponse()

    async def ForwardRequest(self, request, context):
        """Forward request to specific peer."""
        try:
            # Extract request parameters
            request_id = request.request_id
            peer_id = request.peer_id
            request_type = request.request_type
            payload = request.payload

            # Forward the request
            success, response_payload, error = await self.tracker_service.forward_request(
                request_id, peer_id, request_type, payload
            )

            # Create response
            response = pb2.ForwardResponseMessage(request_id=request_id)

            if success and response_payload is not None:
                response.payload = response_payload
            else:
                response.error = error or "Unknown error"

            return response

        except Exception as e:
            logger.error(f"Error in ForwardRequest: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return pb2.ForwardResponseMessage(
                request_id=request.request_id,
                error=f"Internal error: {str(e)}"
            )

    def SubscribeToCatalog(self, request, context):
        """Subscribe to notification when catalog becomes available."""
        try:
            # Extract request parameters
            service_id = request.service_id
            catalog_id = request.catalog_id
            expiration_seconds = request.expiration_seconds

            # Create subscription
            subscription_id = self.tracker_service.create_subscription(
                service_id, catalog_id, expiration_seconds
            )

            if not subscription_id:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Failed to create subscription")
                return pb2.SubscriptionResponse()

            # Check if catalog is already available
            peers = self.tracker_service.get_peers_for_catalog(catalog_id)
            already_available = len(peers) > 0

            # Create response
            response = pb2.SubscriptionResponse(
                subscription_id=subscription_id,
                already_available=already_available
            )

            # If already available, include peers
            if already_available:
                for peer in peers:
                    peer_info = pb2.PeerInfo(
                        peer_id=peer.peer_id,
                        edge_id=peer.edge_id,
                        connection_timestamp=int(peer.connected_at.timestamp()),
                        connection_quality=1.0  # Default quality metric
                    )
                    response.peers.append(peer_info)

            return response

        except Exception as e:
            logger.error(f"Error in SubscribeToCatalog: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return pb2.SubscriptionResponse()
