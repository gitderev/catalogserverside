import AltersideCatalogGenerator from '@/components/AltersideCatalogGenerator';
import { SyncScheduler } from '@/components/SyncScheduler';

const Index = () => {
  return (
    <>
      <SyncScheduler />
      <AltersideCatalogGenerator />
    </>
  );
};

export default Index;
