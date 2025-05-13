import logging
from typing import Dict, Optional, Tuple

import grpc

# Import generated protobuf classes (will be available after protobuf compilation)
# The actual imports will depend on the specific proto files structure
# from src.grpc.generated import edge_pb2 as edge_pb2
# from src.grpc.generated import edge_pb2_grpc as edge_pb2_grpc

logger = logging.getLogger(__name__)


class EdgeServiceClient:
    """Client for communicating with Edge Service."""
    
    def __init__(self):
        # Store channel cache to avoid creating channels for the same edge repeatedly
        self.channels: Dict[str, grpc.Channel] = {}
        self.stubs: Dict[str, Any] = {}
    
    def _get_stub(self, edge_id: str, edge_address: str) -> Any:
        """Get or create a stub for an edge service."""
        if edge_id in self.stubs:
            return self.stubs[edge_id]
        
        # Create channel if it doesn't exist
        if edge_id not in self.channels:
            self.channels[edge_id] = grpc.insecure_channel(edge_address)
        
        # Create stub
        # self.stubs[edge_id] = edge_pb2_grpc.EdgeServiceStub(self.channels[edge_id])
        # Placeholder until we have the actual proto definitions
        self.stubs[edge_id] = None
        
        return self.stubs[edge_id]
    
    def close(self) -> None:
        """Close all channels."""
        for edge_id, channel in self.channels.items():
            channel.close()
        
        self.channels.clear()
        self.stubs.clear()
    
    async def forward_to_peer(
        self,
        edge_id: str,
        edge_address: str,
        peer_id: str,
        request_type: str,
        payload: bytes,
        request_id: str
    ) -> Tuple[bool, Optional[bytes], Optional[str]]:
        """
        Forward a request to a peer via its edge service.
        Returns (success, response_payload, error_message).
        """
        # This is a placeholder implementation. The actual implementation
        # will depend on the specific proto definitions for edge service.
        
        # Example implementation:
        try:
            # Get or create stub
            stub = self._get_stub(edge_id, edge_address)
            
            if stub is None:
                return False, None, "Edge service client not implemented"
            
            # Create request
            # request = edge_pb2.ForwardToPeerRequest(
            #     peer_id=peer_id,
            #     request_type=request_type,
            #     payload=payload,
            #     request_id=request_id
            # )
            
            # Make the call
            # response = await stub.ForwardToPeer(request)
            
            # Process response
            # if response.HasField("error"):
            #     return False, None, response.error
            # else:
            #     return True, response.payload, None
            
            # Placeholder implementation
            return False, None, "Not implemented"
            
        except Exception as e:
            logger.error(f"Error forwarding request to peer {peer_id} via edge {edge_id}: {e}")
            return False, None, f"Error: {str(e)}"
