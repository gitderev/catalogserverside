import AltersideCatalogGenerator from '@/components/AltersideCatalogGenerator';
import { SyncScheduler } from '@/components/SyncScheduler';
import { SyncCronHistory } from '@/components/SyncCronHistory';

const Index = () => {
  return (
    <>
      <SyncScheduler />
      <SyncCronHistory />
      <AltersideCatalogGenerator />
    </>
  );
};

export default Index;
