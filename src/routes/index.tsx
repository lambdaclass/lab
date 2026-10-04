import { createFileRoute } from '@tanstack/react-router';
import { createRedirect } from '@/utils/redirect';

/**
 * The LambdaClass lab only serves the engine timings page, so the landing page (whose
 * cards link to the other pages) redirects there.
 */
export const Route = createFileRoute('/')(createRedirect('/ethereum/execution/timings'));
